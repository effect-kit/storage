import { assert, it } from "@effect/vitest";
import { Clock, Effect, Layer, Redacted } from "effect";
import { KeyValueStore } from "effect/persistence";

import { StorageSharedKeyCredential } from "@azure/storage-blob";

import { AzureStorage } from "../../src/azure.js";
import { AzureSigning } from "../../src/azure/signing.js";
import { AzureUploadGrants } from "../../src/azure/uploads.js";
import { UploadGrantDriver } from "../../src/uploads.js";
import { actor, input, setup } from "./fixture.js";

it.live("Azure browser part grants carry blob-scoped SAS and exact native block receipts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const provider = AzureStorage.layer({
        containerUrl: "https://example.blob.core.windows.net/test",
        storeId: "azure",
      });
      const grants = AzureUploadGrants.layer({ partBytes: 1024 ** 2 }).pipe(
        Layer.provide(
          AzureSigning.layer({
            kind: "shared-key",
            credential: new StorageSharedKeyCredential(
              "example",
              Buffer.from("local-only-signing-fixture").toString("base64"),
            ),
          }).pipe(Layer.provideMerge(provider)),
        ),
      );
      const driver = yield* UploadGrantDriver.pipe(Effect.provide(grants));
      const test = yield* setup();
      const created = yield* test.uploads.create({ actor, input, idempotencyKey: "azure" });
      const record = yield* test.store.read(created.id);
      if (!record || !driver.part) return assert.fail("Missing upload");
      const selected = {
        ...record,
        storeId: "azure",
        input: { ...input, strategy: "multipart" as const },
      };
      const signed = yield* driver.part(
        { ...selected, remote: Redacted.make(yield* driver.begin(selected)) },
        1,
        (yield* Clock.currentTimeMillis) + 60000,
      );
      assert.strictEqual(signed._tag, "SignedRequest");
      if (signed._tag !== "SignedRequest") return;
      const url = new URL(signed.url);
      assert.strictEqual(url.searchParams.get("sr"), "b");
      assert.strictEqual(url.searchParams.get("comp"), "block");
      assert.strictEqual(url.searchParams.get("blockid"), signed.partReceipt);
      assert.strictEqual(url.searchParams.get("spr"), "https");
      assert.isUndefined(signed.headers["x-ms-blob-type"]);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
