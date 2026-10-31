import { assert, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { StoragePublicUrls } from "../../src/core/capabilities.js";
import { AzureStorage } from "../../src/providers/azure/index.js";
import { GcsStorage } from "../../src/providers/gcs/index.js";

it.effect(
  "native public URL layers preserve opaque segments and reject foreign/dot/version refs",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const providers = [
          GcsStorage.publicUrls().pipe(
            Layer.provide(GcsStorage.layer({ bucket: "test-bucket", storeId: "public" })),
          ),
          AzureStorage.publicUrls().pipe(
            Layer.provide(
              AzureStorage.layer({
                containerUrl: "https://example.blob.core.windows.net/test",
                storeId: "public",
              }),
            ),
          ),
        ];
        for (const layer of providers) {
          const context = yield* Layer.build(layer);
          const service = yield* StoragePublicUrls.pipe(Effect.provide(context));
          const url = yield* service.url({ storeId: "public", key: "a //b?%" });
          assert.isTrue(url.endsWith("/a%20//b%3F%25"));
          assert.strictEqual(
            (yield* service.url({ storeId: "foreign", key: "a" }).pipe(Effect.result))._tag,
            "Failure",
          );
          assert.strictEqual(
            (yield* service.url({ storeId: "public", key: "a/../b" }).pipe(Effect.result))._tag,
            "Failure",
          );
        }
      }),
    ),
);
