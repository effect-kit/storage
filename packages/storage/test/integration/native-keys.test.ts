import { assert, it } from "@effect/vitest";
import { Context, Effect, Layer, Stream } from "effect";

import { AzureStorage } from "../../src/providers/azure/index.js";
import { GcsStorage } from "../../src/providers/gcs/index.js";
import * as Runtime from "../../src/runtime/node.js";
import { Transfers, UploadSource } from "../../src/transfers/index.js";
import { httpFixture } from "../fixtures/gcs/http-fixture.js";

it.effect("native key guards reject namespace aliases before SDK requests or source reads", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const azure = yield* AzureStorage.make({
        containerUrl: "http://127.0.0.1:1/account/container",
      });
      const gcs = yield* GcsStorage.make({
        bucket: "test-bucket",
        apiEndpoint: "http://127.0.0.1:1",
        useAuthWithCustomEndpoint: false,
      });
      for (const key of [".", ".."]) {
        const result = yield* gcs.delete({ key }).pipe(Effect.result);
        assert.strictEqual(result._tag === "Failure" && result.failure.reason._tag, "InvalidInput");
      }
      for (const key of [".", "..", "a/../b", "a/./b"]) {
        const result = yield* azure.put({ key, body: new Uint8Array([1]) }).pipe(Effect.result);
        assert.strictEqual(result._tag === "Failure" && result.failure.reason._tag, "InvalidInput");
      }
      const provider = yield* Layer.build(
        AzureStorage.layer({ containerUrl: "http://127.0.0.1:1/account/container" }),
      );
      const transfers = yield* Transfers.make().pipe(Effect.provide(provider));
      let read = false;
      const source = UploadSource.stream(
        Stream.fromEffect(
          Effect.sync(() => {
            read = true;
            return new Uint8Array([1]);
          }),
        ),
        1,
      );
      const result = yield* transfers
        .upload({ key: "a/../b", source, strategy: "blocks" })
        .pipe(Effect.result);
      assert.strictEqual(result._tag === "Failure" && result.failure.reason._tag, "InvalidInput");
      const invalidMetadata = yield* transfers
        .upload({
          key: "safe",
          source,
          strategy: "blocks",
          headers: { customMetadata: { note: "😊" } },
        })
        .pipe(Effect.result);
      assert.strictEqual(
        invalidMetadata._tag === "Failure" && invalidMetadata.failure.reason._tag,
        "InvalidInput",
      );
      assert.isFalse(read);
      assert.strictEqual(Context.get(provider, AzureStorage).provider, "azure");
    }),
  ).pipe(Effect.provide(Runtime.layer)),
);

it.effect("GCS preserves internal dot segments as opaque key bytes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* GcsStorage.make({
        bucket: "test-bucket",
        apiEndpoint: fixture.endpoint,
        useAuthWithCustomEndpoint: false,
      });
      const result = yield* storage.put({ key: "a/../b", body: new Uint8Array([1]) });
      assert.strictEqual((yield* storage.head({ ref: result.ref })).ref.key, "a/../b");
    }),
  ),
);
