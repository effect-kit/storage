import { randomUUID } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { ContainerClient, StorageSharedKeyCredential } from "@azure/storage-blob";

import { storageError } from "../../../src/core/errors.js";
import { Storage } from "../../../src/core/storage.js";
import { AzureStorage } from "../../../src/providers/azure/index.js";
import { aes256Gcm } from "../../../src/runtime/node.js";
import { UploadSessionStore } from "../../../src/sessions/index.js";
import { storageConformanceCases } from "../../../src/testing/index.js";
import { UploadSource } from "../../../src/transfers/index.js";
import { makeSessions } from "../../fixtures/sessions/fixture.js";
import { makeTransfers } from "../../fixtures/transfers/fixture.js";

const baseUrl = process.env.STORAGE_AZURE_LOCAL_URL;
if (!baseUrl || !/^http:\/\/127\.0\.0\.1:\d+\/storagephase$/u.test(baseUrl))
  throw new Error("The owned Azurite endpoint is required");
const acquire = Effect.acquireRelease(
  Effect.promise(async () => {
    const credential = new StorageSharedKeyCredential(
      "storagephase",
      Buffer.from("disposable-local-emulator-key-32").toString("base64"),
    );
    const client = new ContainerClient(
      `${baseUrl}/test${randomUUID().replaceAll("-", "")}`,
      credential,
      { retryOptions: { maxTries: 1 } },
    );
    await client.create();
    return await Effect.runPromise(
      AzureStorage.makeClient(
        client,
        { containerUrl: client.url, storeId: randomUUID() },
        { sdkRetries: "disabled" },
      ),
    );
  }),
  (storage) =>
    Effect.promise(async () => {
      await storage.client.delete();
    }),
);

for (const test of storageConformanceCases)
  it.effect(`Azurite: ${test.name}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* acquire;
        yield* test.run(storage);
      }),
    ),
  );

it.effect(
  "Azurite stages named blocks, preserves existing blobs on session abort, and enforces leases",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* acquire;
        const original = yield* storage.put({ key: "block", body: new Uint8Array([1]) });
        const ref = { storeId: storage.storeId, key: "block" };
        yield* storage.blocks.stage({
          ref,
          blockId: "YmxvY2swMQ==",
          body: new Uint8Array([2]),
          contentLength: 1,
        });
        assert.strictEqual((yield* storage.blocks.list(ref)).uncommitted.length, 1);
        const lease = yield* storage.acquireLease({ ref, durationSeconds: 15 });
        const rejected = yield* storage
          .put({ key: "block", body: new Uint8Array([3]) })
          .pipe(Effect.result);
        assert.strictEqual(
          rejected._tag === "Failure" && rejected.failure.reason._tag,
          "PreconditionFailed",
        );
        yield* storage.renewLease({ ref, leaseId: lease });
        yield* storage.releaseLease({ ref, leaseId: lease });
        const driver = {
          storage,
          blocks: storage.blocks,
          maximumObjectBytes: 1024,
          maximumSingleUploadBytes: 1024,
          transportOverheadBytes: 1,
        };
        const store = yield* UploadSessionStore.make({
          namespace: "azure",
          protection: aes256Gcm(Redacted.make(new Uint8Array(32).fill(1))),
          ownership: "exclusive",
        });
        yield* storage.put({ key: "durable-abort", body: new Uint8Array([1]) });
        let stages = 0;
        const sessions = yield* makeSessions(
          {
            ...driver,
            blocks: {
              ...storage.blocks,
              stage: (input) =>
                ++stages === 2
                  ? Effect.fail(
                      storageError("stage", "azure", { _tag: "TransportFailure" }, "unknown"),
                    )
                  : storage.blocks.stage(input),
            },
          },
          store,
          { configurationId: "local" },
        );
        const source = UploadSource.bytes(new Uint8Array(48).fill(9));
        const id = yield* sessions.create({ key: "durable-abort", source });
        assert.strictEqual(
          (yield* sessions.resume(id, source).pipe(Effect.result))._tag,
          "Failure",
        );
        yield* sessions.abort(id);
        assert.strictEqual(
          (yield* storage.blocks.list({ storeId: storage.storeId, key: "durable-abort" }))
            .uncommitted.length,
          1,
        );
        assert.deepStrictEqual(
          Array.from(
            yield* Storage.getBytes({ key: "durable-abort", maxBytes: 8 }).pipe(
              Effect.provideService(Storage, storage),
            ),
          ),
          [1],
        );
        assert.deepStrictEqual(
          Array.from(
            yield* Storage.getBytes({ key: "block", maxBytes: 8 }).pipe(
              Effect.provideService(Storage, storage),
            ),
          ),
          [1],
        );
        assert.strictEqual((yield* storage.head({ ref: original.ref })).size, 1);
      }),
    ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("Azurite managed blocks and durable restart verify complete bytes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* acquire;
      const driver = {
        storage,
        blocks: storage.blocks,
        maximumObjectBytes: 1024,
        maximumSingleUploadBytes: 1024,
        transportOverheadBytes: 1,
      };
      const transfers = yield* makeTransfers(driver);
      const receipt = yield* transfers.upload({
        key: "managed",
        strategy: "blocks",
        source: UploadSource.bytes(new Uint8Array(48).fill(7)),
      });
      assert.strictEqual(receipt.strategy, "blocks");
      assert.strictEqual(receipt.metadata.size, 48);
      const h = yield* storage.head({ key: "managed" });
      const g = yield* storage.get({ key: "managed" });
      assert.strictEqual(h.etag, g.metadata.etag);
      const store = yield* UploadSessionStore.make({
        namespace: "azure",
        protection: aes256Gcm(Redacted.make(new Uint8Array(32).fill(1))),
        ownership: "exclusive",
      });
      const first = yield* makeSessions(driver, store, { configurationId: "local" });
      const source = UploadSource.bytes(new Uint8Array(48).fill(4));
      const id = yield* first.create({ key: "durable", source });
      const restarted = yield* makeSessions(driver, store, { configurationId: "local" });
      const result = yield* restarted.resume(id, source);
      assert.strictEqual(result.size, 48);
      assert.strictEqual((yield* restarted.status(id)).state, "completed");
      const body = yield* storage.get({ ref: result.ref });
      if (body._tag !== "Object") return assert.fail("Expected object");
      assert.strictEqual(
        (yield* Stream.runCollect(body.body)).reduce((total, chunk) => total + chunk.length, 0),
        48,
      );
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
