import { assert, it } from "@effect/vitest";
import { Clock, Effect, Layer, Redacted, Result } from "effect";
import { KeyValueStore } from "effect/persistence";

import { S3Storage } from "../../src/s3.js";
import { S3UploadGrants } from "../../src/s3/uploads.js";
import { UploadGrantDriver } from "../../src/uploads.js";
import { httpFixture, xml } from "../s3/http-fixture.js";
import { actor, input, setup } from "./fixture.js";

it.effect("S3 browser grants bind create-only single uploads and SHA-256 multipart parts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let starts = 0;
      const fixture = yield* httpFixture((incoming, response) => {
        incoming.resume();
        if (incoming.method === "POST") {
          starts++;
          xml(
            response,
            "<InitiateMultipartUploadResult><Bucket>storage-tests</Bucket><Key>file</Key><UploadId>native-upload</UploadId></InitiateMultipartUploadResult>",
          );
        } else xml(response, "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>");
      });
      const driver = yield* UploadGrantDriver.pipe(
        Effect.provide(
          S3UploadGrants.layer({ partBytes: 5 * 1024 ** 2 }).pipe(
            Layer.provide(Layer.succeed(S3Storage, fixture.storage)),
          ),
        ),
      );
      const test = yield* setup();
      const created = yield* test.uploads.create({ actor, input, idempotencyKey: "signed" });
      const record = yield* test.store.read(created.id);
      if (!record) return assert.fail("Missing record");
      const single = {
        ...record,
        storeId: fixture.storage.storeId,
        input: { ...record.input, strategy: "single" as const },
      };
      const singleGrant = yield* driver.grant(
        { ...single, remote: Redacted.make(yield* driver.begin(single)) },
        (yield* Clock.currentTimeMillis) + 60000,
      );
      assert.strictEqual(singleGrant._tag, "SignedRequest");
      if (singleGrant._tag === "SignedRequest") {
        assert.strictEqual(singleGrant.headers["if-none-match"], "*");
        assert.include(singleGrant.url, "X-Amz-Signature");
      }
      const multipart = {
        ...single,
        input: {
          ...single.input,
          strategy: "multipart" as const,
          file: { ...single.input.file, size: 6 * 1024 ** 2 },
        },
      };
      const remote = yield* driver.begin(multipart);
      const selected = { ...multipart, remote: Redacted.make(remote) };
      const grant = yield* driver.grant(selected, (yield* Clock.currentTimeMillis) + 60000);
      assert.strictEqual(grant._tag, "Multipart");
      assert.strictEqual(starts, 1);
      if (!driver.part) return assert.fail("Missing part signer");
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(driver.part(selected, 1, (yield* Clock.currentTimeMillis) + 60000)),
        ),
      );
      const signed = yield* driver.part(
        selected,
        1,
        (yield* Clock.currentTimeMillis) + 60000,
        "1".repeat(64),
      );
      assert.strictEqual(signed._tag, "SignedRequest");
      if (signed._tag === "SignedRequest")
        assert.strictEqual(
          signed.headers["x-amz-checksum-sha256"],
          Buffer.from("1".repeat(64), "hex").toString("base64"),
        );
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
