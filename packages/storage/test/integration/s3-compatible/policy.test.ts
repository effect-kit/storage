import { assert, it } from "@effect/vitest";
import type { Scope } from "effect";
import { Context, Effect, Layer, Redacted, Result, Stream } from "effect";

import type { StorageError } from "../../../src/core/errors.js";
import { Storage } from "../../../src/core/storage.js";
import { HetznerStorage } from "../../../src/providers/hetzner/index.js";
import { R2Storage } from "../../../src/providers/r2/index.js";
import { S3Signing } from "../../../src/providers/s3/signing.js";
import { makeS3TransferDriver } from "../../../src/providers/s3/transfer-driver.js";
import { ScalewayStorage } from "../../../src/providers/scaleway/index.js";
import { SpacesStorage } from "../../../src/providers/spaces/index.js";
import { TigrisStorage } from "../../../src/providers/tigris/index.js";
import { TransferDriver } from "../../../src/transfers/dependencies.js";
import { settings } from "../../fixtures/s3-compatible/profiles.js";
import { drain, httpFixture, xml } from "../../fixtures/s3/http-fixture.js";

it.effect(
  "Scaleway limits and checksum types guard primitives, parts, signing and completion",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let touched = 0;
        const source = Stream.fromEffect(
          Effect.sync(() => {
            touched++;
            return new Uint8Array();
          }),
        );
        const storage = yield* ScalewayStorage.make({ ...settings(), region: "fr-par" });
        const upload = {
          ref: { storeId: storage.storeId, key: "key" },
          uploadId: Redacted.make("opaque"),
        };
        assert.strictEqual(storage.multipart.limits.maximumParts, 1000);
        for (const operation of [
          storage.multipart.uploadPart({ upload, body: source, partNumber: 1001 }),
          storage.multipart.uploadPart({
            upload,
            body: source,
            partNumber: 1,
            contentLength: 5_000_000_001,
          }),
          storage.multipart.complete({ upload, parts: [{ partNumber: 1001, etag: '"part"' }] }),
          storage.multipart.initiate({
            key: "key",
            checksum: { ChecksumAlgorithm: "CRC64NVME", ChecksumType: "COMPOSITE" },
          }),
          storage.multipart.initiate({
            key: "key",
            checksum: { ChecksumAlgorithm: "SHA256", ChecksumType: "FULL_OBJECT" },
          }),
          S3Signing.make(storage).signPart({ upload, partNumber: 1001, expiresInSeconds: 60 }),
        ]) {
          const result = yield* Effect.result(
            Effect.asVoid<unknown, StorageError, Scope.Scope>(operation),
          );
          assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "Unsupported");
        }
        assert.strictEqual(touched, 0);
      }),
    ),
);

it.effect("R2 rejects AWS encryption, versions and conditional completion without a request", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* R2Storage.make({ ...settings(), accountId: "a".repeat(32) });
      const ref = {
        storeId: storage.storeId,
        key: "key",
        version: { _tag: "S3Version" as const, id: "unrelated" },
      };
      for (const operation of [
        storage.get({ ref }),
        storage.head({ ref }),
        storage.delete({ ref }),
        storage.deleteObjects([{ ref }]),
        S3Signing.make(storage).sign({ ref, method: "GET", expiresInSeconds: 60 }),
        storage.multipart.complete({
          upload: { ref: { ...ref, version: undefined }, uploadId: Redacted.make("id") },
          parts: [{ partNumber: 1, etag: '"part"' }],
          condition: { _tag: "CreateOnly" },
        }),
      ]) {
        const result = yield* Effect.result(
          Effect.asVoid<unknown, StorageError, Scope.Scope>(operation),
        );
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "Unsupported");
      }
      const denied = yield* Effect.result(
        R2Storage.make({
          ...settings(),
          accountId: "a".repeat(32),
          encryption: { mode: "aws:kms", keyId: "not-a-secret" },
        }),
      );
      assert.isTrue(Result.isFailure(denied) && denied.failure.reason._tag === "Unsupported");
      assert.isUndefined(makeS3TransferDriver(storage).nativeCopy?.part);
    }),
  ),
);

it.effect("Tigris placement gates atomic writes and rejects ineffective encryption requests", () =>
  Effect.scoped(
    Effect.gen(function* () {
      for (const placement of ["single-region", "multi-region", "global", "dual-region"] as const) {
        const storage = yield* TigrisStorage.make({ ...settings(), placement });
        assert.strictEqual(
          storage.capabilities.putConditions.length,
          ["global", "dual-region"].includes(placement) ? 0 : 2,
        );
      }
      const denied = yield* Effect.result(
        TigrisStorage.make({ ...settings(), placement: "global", encryption: { mode: "AES256" } }),
      );
      assert.isTrue(Result.isFailure(denied) && denied.failure.reason._tag === "Unsupported");
    }),
  ),
);

it.effect("Spaces Cold and Hetzner same-bucket restrictions also control managed native copy", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cold = yield* SpacesStorage.make({ ...settings(), region: "nyc3", tier: "cold" });
      assert.isFalse(makeS3TransferDriver(cold).nativeCopy?.accepts(makeS3TransferDriver(cold)));
      const source = yield* HetznerStorage.make({
        ...settings(),
        region: "fsn1",
        versioning: true,
      });
      const destination = yield* HetznerStorage.make({
        ...settings(),
        bucket: "other-bucket",
        region: "fsn1",
        versioning: true,
      });
      assert.isFalse(
        makeS3TransferDriver(destination).nativeCopy?.accepts(makeS3TransferDriver(source)),
      );
      const outcome = yield* Effect.result(
        destination.copyObject(
          { source: { storeId: source.storeId, key: "key" }, destinationKey: "copy" },
          source,
        ),
      );
      assert.isTrue(Result.isFailure(outcome) && outcome.failure.reason._tag === "Unsupported");
    }),
  ),
);

it.effect(
  "named layers share storage, S3 signing authority and transfer policy without extra clients",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(
          ScalewayStorage.layer({ ...settings(), region: "fr-par" }),
        );
        const named = Context.get(context, ScalewayStorage);
        assert.strictEqual(Context.get(context, Storage), named);
        const driver = Context.get(context, TransferDriver);
        assert.strictEqual(driver.storage, named);
        assert.strictEqual(driver.multipart?.limits.maximumParts, 1000);
        assert.strictEqual(driver.maximumObjectBytes, 5_000_000_000_000);
      }),
    ),
);

it.effect("profile changes invalidate cursors even with an explicit shared store ID", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture((_incoming, response) =>
        xml(
          response,
          "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>remote-token</NextContinuationToken></ListBucketResult>",
        ),
      );
      const standard = yield* SpacesStorage.make({
        ...settings(fixture.endpoint),
        region: "nyc3",
        tier: "standard",
        storeId: "shared",
      });
      const cold = yield* SpacesStorage.make({
        ...settings(fixture.endpoint),
        region: "nyc3",
        tier: "cold",
        storeId: "shared",
      });
      const page = yield* standard.listPage();
      const result = yield* Effect.result(cold.listPage({ cursor: page.cursor }));
      assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "InvalidInput");
    }),
  ),
);

it.effect(
  "Scaleway keeps checksum protection while compatible profiles avoid unqualified SDK defaults",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let automatic: string | string[] | undefined;
        let completionCondition: string | string[] | undefined;
        const fixture = yield* httpFixture((incoming, response) => {
          automatic = incoming.headers["x-amz-checksum-crc32"];
          completionCondition = incoming.headers["if-none-match"];
          void drain(incoming).then(() => {
            if (incoming.method === "POST")
              xml(
                response,
                '<CompleteMultipartUploadResult><ETag>"done"</ETag></CompleteMultipartUploadResult>',
              );
            else {
              response.writeHead(200, { etag: '"saved"' });
              response.end();
            }
            return undefined;
          });
        });
        const scw = yield* ScalewayStorage.make({
          ...settings(fixture.endpoint),
          region: "fr-par",
        });
        yield* scw.put({ key: "checksum", body: new Uint8Array([1]) });
        assert.isDefined(automatic);
        const r2 = yield* R2Storage.make({
          ...settings(fixture.endpoint),
          accountId: "a".repeat(32),
        });
        yield* r2.put({ key: "checksum", body: new Uint8Array([1]) });
        assert.isUndefined(automatic);
        yield* scw.multipart.complete({
          upload: { ref: { storeId: scw.storeId, key: "key" }, uploadId: Redacted.make("upload") },
          parts: [{ partNumber: 1, etag: '"part"', size: 1 }],
          condition: { _tag: "CreateOnly" },
        });
        assert.strictEqual(completionCondition, "*");
      }),
    ),
);

it.effect("durable handles reject a changed named configuration before contacting storage", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const standard = yield* SpacesStorage.make({
        ...settings(),
        storeId: "same",
        region: "nyc3",
        tier: "standard",
      });
      const cold = yield* SpacesStorage.make({
        ...settings(),
        storeId: "same",
        region: "nyc3",
        tier: "cold",
      });
      const original = makeS3TransferDriver(standard).handles;
      const changed = makeS3TransferDriver(cold).handles;
      assert.isDefined(original);
      assert.isDefined(changed);
      if (!original || !changed) return;
      const encoded = yield* original.encode({
        ref: { storeId: "same", key: "key" },
        uploadId: Redacted.make("private-id"),
      });
      const roundTrip = yield* original.decode(encoded);
      assert.strictEqual(Redacted.value(roundTrip.uploadId), "private-id");
      const refused = yield* Effect.result(changed.decode(encoded));
      assert.isTrue(Result.isFailure(refused) && refused.failure.reason._tag === "InvalidSession");
    }),
  ),
);

it.effect(
  "default hosts and signing regions follow the selected provider rather than ambient AWS",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const r2 = yield* R2Storage.make({
          ...settings(),
          accountId: "a".repeat(32),
          jurisdiction: "eu",
        });
        assert.strictEqual(
          r2.config.endpoint,
          `https://${"a".repeat(32)}.eu.r2.cloudflarestorage.com`,
        );
        assert.strictEqual(r2.config.region, "auto");
        const spaces = yield* SpacesStorage.make({
          ...settings(),
          region: "nyc3",
          tier: "standard",
        });
        assert.strictEqual(spaces.config.endpoint, "https://nyc3.digitaloceanspaces.com");
        assert.strictEqual(spaces.config.region, "us-east-1");
        const tigris = yield* TigrisStorage.make({ ...settings(), placement: "global" });
        assert.strictEqual(tigris.config.endpoint, "https://t3.storage.dev");
        assert.isFalse(Object.hasOwn(tigris.config, "credentials"));
      }),
    ),
);

it.effect(
  "malformed authority and nonlocal plaintext endpoints fail with safe expected errors",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const config of [
          {
            ...settings(),
            region: "fr-par",
            credentials: { accessKeyId: "", secretAccessKey: "private-test-value" },
          },
          { ...settings("http://remote.example.test"), region: "fr-par" },
          {
            ...settings("https://gateway.example.test/?token=private-test-value"),
            region: "fr-par",
          },
        ]) {
          const result = yield* Effect.result(ScalewayStorage.make(config));
          assert.isTrue(Result.isFailure(result));
          if (Result.isFailure(result)) {
            assert.strictEqual(result.failure.provider, "scaleway");
            assert.strictEqual(result.failure.reason._tag, "InvalidConfiguration");
            assert.notInclude(JSON.stringify(result.failure), "private-test-value");
          }
        }
      }),
    ),
);
