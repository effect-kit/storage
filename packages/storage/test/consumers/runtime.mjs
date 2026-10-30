import assert from "node:assert/strict";
import { createHash, createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { createRequire } from "node:module";

import { Context, Effect, Layer, Redacted, Result, Schema, Stream } from "effect";
import { SecureRandom } from "effect/crypto";
import { KeyValueStore } from "effect/persistence";

import { ObjectKey, ObjectSize, Storage, StoragePublicUrls } from "@effect-kit/storage";
import { UploadSessions, UploadSessionStore } from "@effect-kit/storage/sessions";
import { Faults, MemoryStorage, runStorageConformance } from "@effect-kit/storage/testing";
import {
  TransferDriver,
  TransferHash,
  Transfers,
  UploadSource,
} from "@effect-kit/storage/transfers";

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
  "@azure/identity",
  "@effect/sql-pg",
  "@vercel/blob",
  "@supabase/storage-js",
]) {
  assert.throws(() => require.resolve(peer));
}

const checksum = (value) => ({
  algorithm: "sha256",
  encoding: "hex",
  scope: "full-object",
  value,
});

const hash = {
  algorithm: "sha256",
  workspaceBytes: 32768,
  create: () => {
    const state = createHash("sha256");
    return {
      update: (bytes) => {
        state.update(bytes);
      },
      finish: () => checksum(state.digest("hex")),
      snapshot: () => checksum(state.copy().digest("hex")),
    };
  },
};
const secret = randomBytes(32);
const protection = {
  seal: (text, aad) =>
    Effect.sync(() => {
      const iv = randomBytes(12);
      const cipher = createCipheriv("aes-256-gcm", secret, iv);
      cipher.setAAD(Buffer.from(aad));
      const body = Buffer.concat([cipher.update(text, "utf8"), cipher.final()]);
      return Redacted.make(
        JSON.stringify({
          iv: iv.toString("base64"),
          tag: cipher.getAuthTag().toString("base64"),
          body: body.toString("base64"),
        }),
      );
    }),
  open: (encoded, aad) =>
    Effect.sync(() => {
      const value = JSON.parse(Redacted.value(encoded));
      const cipher = createDecipheriv("aes-256-gcm", secret, Buffer.from(value.iv, "base64"));
      cipher.setAAD(Buffer.from(aad));
      cipher.setAuthTag(Buffer.from(value.tag, "base64"));
      return Buffer.concat([
        cipher.update(Buffer.from(value.body, "base64")),
        cipher.final(),
      ]).toString("utf8");
    }),
};
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const memory = yield* MemoryStorage.make();
      const managed = yield* Layer.build(
        Transfers.layer({ maxBytes: 1024 }).pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(Storage, memory),
              Layer.succeed(TransferDriver, {
                storage: memory,
                maximumObjectBytes: 1024,
                maximumSingleUploadBytes: 1024,
                transportOverheadBytes: 0,
              }),
              Layer.succeed(TransferHash, hash),
              SecureRandom.layerWebCrypto,
            ),
          ),
        ),
      );
      const transfers = Context.get(managed, Transfers);
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
        protection,
      }).pipe(Effect.provide(KeyValueStore.layerMemory));
      const sessions = yield* UploadSessions.make({ configurationId: "packed-memory" }).pipe(
        Effect.provide(managed),
        Effect.provideService(UploadSessionStore, checkpointStore),
      );
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
        const receipt = yield* storage.put({
          key: "hello",
          body: new TextEncoder().encode("hello"),
        });
        assert.equal(yield* Storage.getText({ key: "hello", maxBytes: 5 }), "hello");
        const resolver = yield* StoragePublicUrls;
        assert.equal(yield* resolver.url(receipt.ref), "https://example.invalid/hello");
      });
      yield* program.pipe(
        Effect.provide(Layer.merge(Layer.succeed(Storage, custom), publicUrls(custom))),
      );
    }),
  ),
);

assert.equal(
  typeof (await import("@effect-kit/storage/r2-workers")).R2WorkersStorage.layer,
  "function",
);

await Promise.all(
  [
    "lifecycle",
    "events",
    "session-events",
    "r2/admin",
    "r2/events",
    "supabase/admin",
    "b2/admin",
    "s3/events",
    "gcs/events",
    "azure/events",
    "tigris/events",
  ].map(async (path) => assert(Object.keys(await import(`@effect-kit/storage/${path}`)).length)),
);
