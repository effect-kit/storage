import { assert, it } from "@effect/vitest";
import { Context, Effect, Layer, Option, Redacted } from "effect";

import { Storage, StorageSigning } from "@effect-kit/storage";

import type { ProviderSettings } from "../../../../../scripts/providers/configuration.ts";
import { providerLayer } from "../../e2e/live/adapters.js";
import { fixtureCredentials } from "../../fixtures/s3/http-fixture.js";

const profiles: ReadonlyArray<{
  provider: ProviderSettings["provider"];
  config: ProviderSettings["config"];
  signingRegion: string;
}> = [
  {
    provider: "s3",
    config: { bucket: "signing-fixture", region: "eu-north-1" },
    signingRegion: "eu-north-1",
  },
  {
    provider: "r2",
    config: { bucket: "signing-fixture", accountId: "a".repeat(32) },
    signingRegion: "auto",
  },
];

for (const { provider, config, signingRegion } of profiles) {
  it.effect(
    `live ${provider} signing profile acquires its signer and binds a GET to the store`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const credentials = fixtureCredentials();
          const layer = yield* Effect.promise(() =>
            providerLayer(
              {
                provider,
                config,
                prefix: "effect-storage-acceptance/unit/",
                secrets: {
                  ACCESS_KEY_ID: credentials.accessKeyId,
                  SECRET_ACCESS_KEY: credentials.secretAccessKey,
                },
              },
              "signing",
            ),
          );
          const context = yield* Layer.build(layer);
          const storage = Context.get(context, Storage);
          const signer = Option.getOrThrow(Context.getOption(context, StorageSigning));
          const ref = { storeId: storage.storeId, key: "private/object" };
          const request = yield* signer.sign({ ref, method: "GET", expiresInSeconds: 60 });

          assert.deepStrictEqual(request.ref, ref);
          const url = new URL(Redacted.value(request.url));
          assert.strictEqual(request.method, "GET");
          assert.strictEqual(url.protocol, "https:");
          assert.strictEqual(url.searchParams.get("X-Amz-Algorithm"), "AWS4-HMAC-SHA256");
          assert.isTrue(
            url.searchParams.get("X-Amz-Credential")?.endsWith(`/${signingRegion}/s3/aws4_request`),
          );
          assert.isNotEmpty(url.searchParams.get("X-Amz-Signature"));
        }),
      ),
  );
}
