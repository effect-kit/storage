import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { Effect, Layer, Redacted, Result, Schema, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { ObjectKey, ObjectSize, Storage, StoragePublicUrls } from "@effect-kit/storage";
import { UploadSessions, UploadSessionStore } from "@effect-kit/storage/sessions";
import { aes256Gcm } from "@effect-kit/storage/sessions/node";
import { Faults, MemoryStorage, runStorageConformance } from "@effect-kit/storage/testing";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";
import { sha256 } from "@effect-kit/storage/transfers/node";

import { makeCustomAdapter, publicUrls } from "./build/custom-adapter.js";

assert.equal(Schema.decodeUnknownSync(ObjectKey)("a//b"), "a//b");
assert.equal(Schema.decodeUnknownSync(ObjectSize)(0), 0);
assert.throws(() => Schema.decodeUnknownSync(ObjectSize)(Number.MAX_SAFE_INTEGER + 1));
const require = createRequire(import.meta.url);
for (const peer of [
  "@aws-sdk/client-s3",
  "@aws-sdk/s3-request-presigner",
  "@aws-sdk/s3-presigned-post",
  "@google-cloud/storage",
  "@azure/storage-blob",
  "@effect/sql-pg",
  "@vercel/blob",
  "@supabase/storage-js",
]) {
  assert.throws(() => require.resolve(peer));
}

await Effect.runPromise(
  Effect.gen(function* () {
    const memory = yield* MemoryStorage.make();
    const transfers = yield* Transfers.make(
      {
        storage: memory,
        maximumObjectBytes: 1024,
        maximumSingleUploadBytes: 1024,
        transportOverheadBytes: 0,
      },
      { digest: sha256, maxBytes: 1024 },
    );
    const transfer = yield* transfers.upload({
      key: "managed",
      source: UploadSource.bytes(new Uint8Array([1, 2])),
    });
    const verified = yield* transfers.download({
      ref: transfer.metadata.ref,
      expectedChecksum: transfer.checksum,
      target: { write: () => Effect.void },
    });
    assert.equal(verified.progress.bytesVerified, 2);
    const checkpointStore = yield* UploadSessionStore.make({
      namespace: "packed",
      ownership: "exclusive",
      protection: aes256Gcm(Redacted.make(globalThis.crypto.getRandomValues(new Uint8Array(32)))),
    }).pipe(Effect.provide(KeyValueStore.layerMemory));
    const sessions = yield* UploadSessions.make(
      transfers.endpoint,
      { digest: sha256, maxBytes: 1024 },
      {
        configurationId: "packed-memory",
        handles: {
          encode: () => Effect.die("download has no handle"),
          decode: () => Effect.die("download has no handle"),
        },
      },
    ).pipe(Effect.provideService(UploadSessionStore, checkpointStore));
    yield* sessions.createDownload({
      id: "download",
      ref: transfer.metadata.ref,
      expectedChecksum: transfer.checksum,
    });
    const downloaded = [];
    yield* sessions.resumeDownload("download", {
      id: "download",
      prefix: () => Stream.empty,
      truncate: () => Effect.void,
      write: (_offset, bytes) =>
        Effect.sync(() => {
          downloaded.push(...bytes);
        }),
      sync: Effect.void,
      publish: () => Effect.void,
    });
    assert.deepEqual(downloaded, [1, 2]);
    assert.equal((yield* sessions.status("download")).state, "completed");

    const faults = yield* Faults.make(memory, [
      {
        operation: "put",
        afterFailure: { _tag: "TransportFailure", code: "LostResponse" },
      },
    ]);
    const outcome = yield* Effect.result(
      faults.storage.put({
        key: "committed",
        body: new Uint8Array([1]),
      }),
    );
    assert.ok(Result.isFailure(outcome) && outcome.failure.mutationOutcome === "unknown");
    assert.equal((yield* memory.head({ key: "committed" })).size, 1);
    yield* faults.assertDone;
    const memoryCases = yield* runStorageConformance(() => MemoryStorage.make());
    assert.ok(memoryCases.every((test) => test.status === "passed"));
    const customCases = yield* runStorageConformance(makeCustomAdapter);
    assert.ok(customCases.some((test) => test.status === "passed"));
    assert.ok(customCases.some((test) => test.status === "skipped"));
    const custom = yield* makeCustomAdapter();
    const program = Effect.gen(function* () {
      const storage = yield* Storage;
      const receipt = yield* storage.put({ key: "hello", body: new TextEncoder().encode("hello") });
      assert.equal(yield* Storage.getText({ key: "hello", maxBytes: 5 }), "hello");
      const resolver = yield* StoragePublicUrls;
      assert.equal(yield* resolver.url(receipt.ref), "https://example.invalid/hello");
    });
    yield* program.pipe(
      Effect.provide(Layer.merge(Layer.succeed(Storage, custom), publicUrls(custom))),
    );
  }),
);
