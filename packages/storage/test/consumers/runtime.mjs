import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { Effect, Layer, Result, Schema } from "effect";

import { ObjectKey, ObjectSize, Storage, StoragePublicUrls } from "@effect-kit/storage";
import { Faults, MemoryStorage, runStorageConformance } from "@effect-kit/storage/testing";

import { makeCustomAdapter, publicUrls } from "./build/custom-adapter.js";

assert.equal(Schema.decodeUnknownSync(ObjectKey)("a//b"), "a//b");
assert.equal(Schema.decodeUnknownSync(ObjectSize)(0), 0);
assert.throws(() => Schema.decodeUnknownSync(ObjectSize)(Number.MAX_SAFE_INTEGER + 1));
const require = createRequire(import.meta.url);
for (const peer of [
  "@aws-sdk/client-s3",
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
