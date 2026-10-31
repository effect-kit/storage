import { assert, it } from "@effect/vitest";
import { DateTime, Effect, Layer, Redacted } from "effect";

import { StorageSharedKeyCredential } from "@azure/storage-blob";

import { StorageSigning } from "../../../src/core/capabilities.js";
import { AzureStorage } from "../../../src/providers/azure/index.js";
import { AzureSigning } from "../../../src/providers/azure/signing.js";

it.effect(
  "Azure SAS caps credential expiry and preserves snapshot identity without identity peer",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const provider = AzureStorage.layer({
          containerUrl: "https://example.blob.core.windows.net/test",
          storeId: "azure",
        });
        const authority = {
          kind: "shared-key" as const,
          credential: new StorageSharedKeyCredential(
            "example",
            Buffer.from("local-only-signing-fixture").toString("base64"),
          ),
          expiresAt: DateTime.makeUnsafe(30_000),
        };
        const signing = yield* Layer.build(
          AzureSigning.layer(authority).pipe(Layer.provide(provider)),
        );
        const grant = yield* Effect.flatMap(StorageSigning, (service) =>
          service.sign({
            ref: {
              storeId: "azure",
              key: "a/b",
              version: { _tag: "AzureSnapshot", id: "2026-10-30T00:00:00.0000000Z" },
            },
            method: "GET",
            expiresInSeconds: 60,
          }),
        ).pipe(Effect.provide(signing));
        assert.strictEqual(DateTime.toEpochMillis(grant.expiresAt), 30_000);
        const url = new URL(Redacted.value(grant.url));
        assert.strictEqual(url.searchParams.get("snapshot"), "2026-10-30T00:00:00.0000000Z");
        assert.strictEqual(url.searchParams.get("sr"), "bs");
        assert.strictEqual(url.searchParams.get("sp"), "r");
        assert.isFalse(JSON.stringify(grant).includes("sig="));
      }),
    ),
);
