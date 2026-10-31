import { assert, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";
import { FetchHttpClient } from "effect/http";
import { KeyValueStore } from "effect/persistence";

import { storageError } from "../../../src/core/errors.js";
import { SupabaseStorage } from "../../../src/providers/supabase/index.js";
import { aes256Gcm } from "../../../src/runtime/node.js";
import { UploadSessionStore } from "../../../src/sessions/index.js";
import { UploadSource } from "../../../src/transfers/index.js";
import { makeSessions } from "../../fixtures/sessions/fixture.js";
import { httpFixture } from "../../fixtures/supabase/http-fixture.js";
import { makeTransfers } from "../../fixtures/transfers/fixture.js";

const policy = {
  memoryBudgetBytes: 40 * 1024 ** 2,
  maxBytes: 10 * 1024 ** 2,
  maxSourceChunkBytes: 1024 ** 2,
  retryBaseMillis: 0,
};
it.effect(
  "a gateway OPTIONS response allows creation without inventing optional tus extensions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        fixture.tusOptions({});
        const storage = yield* SupabaseStorage.make({
          url: fixture.url,
          bucket: "bucket",
          storeId: "test",
          auth: { mode: "user", token: Redacted.make("user") },
          resumable: true,
        });
        assert(storage.offsets);
        const upload = yield* storage.offsets.create({ key: "gateway", contentLength: 1 });
        const result = yield* storage.offsets.write({
          upload,
          offset: 0,
          body: new Uint8Array([1]),
          contentLength: 1,
          final: true,
        });
        assert.equal(result._tag, "Completed");
        const before = fixture.seen.length;
        const unsupported = yield* Effect.result(storage.offsets.cancel(upload));
        assert.equal(unsupported._tag, "Failure");
        if (unsupported._tag === "Failure")
          assert.equal(unsupported.failure.reason._tag, "Unsupported");
        assert.equal(fixture.seen.length, before + 1);
        assert.equal(fixture.seen.at(-1)?.method, "HEAD");

        fixture.tusOptions({ "tus-version": "2.0.0", "tus-extension": "creation" });
        const incompatible = yield* Effect.result(
          SupabaseStorage.make({
            url: fixture.url,
            bucket: "bucket",
            storeId: "test",
            auth: { mode: "user", token: Redacted.make("user") },
            resumable: true,
          }),
        );
        assert.equal(incompatible._tag, "Failure");
      }),
    ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect("protects tus URLs and resumes a saved offset, including a lost final response", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* SupabaseStorage.make({
        url: fixture.url,
        bucket: "bucket",
        storeId: "test",
        auth: { mode: "user", token: Redacted.make("user") },
        resumable: true,
      });
      if (!storage.offsets) return assert.fail("Missing negotiated tus");
      const offsets = storage.offsets;
      const endpoint = {
        storage,
        offsets,
        maximumObjectBytes: policy.maxBytes,
        maximumSingleUploadBytes: policy.maxBytes,
        transportOverheadBytes: 1024,
      };
      const kvs = yield* KeyValueStore.KeyValueStore;
      const store = yield* UploadSessionStore.make({
        namespace: "tus",
        ownership: "exclusive",
        protection: aes256Gcm(Redacted.make(new Uint8Array(32).fill(9))),
      });
      const source = UploadSource.bytes(new Uint8Array(7 * 1024 ** 2).fill(7));
      let count = 0;
      const first = yield* makeSessions(
        {
          ...endpoint,
          offsets: {
            ...offsets,
            write: (input) =>
              ++count === 2
                ? Effect.fail(
                    storageError("write", "supabase", { _tag: "ProviderFailure" }, "rejected"),
                  )
                : offsets.write(input),
          },
        },
        store,
        { configurationId: "tus" },
        policy,
      );
      const id = yield* first.create({ key: "restart", source });
      assert.strictEqual((yield* first.resume(id, source).pipe(Effect.result))._tag, "Failure");
      assert.isFalse((yield* kvs.get(`tus:${id}`))?.includes(fixture.url));
      const second = yield* makeSessions(endpoint, store, { configurationId: "tus" }, policy);
      const before = fixture.seen.length;
      assert.strictEqual(
        (yield* second
          .resume(id, UploadSource.bytes(new Uint8Array(7 * 1024 ** 2).fill(8)))
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(fixture.seen.length, before);
      fixture.losePatch();
      const result = yield* second.resume(id, source);
      assert.strictEqual(result.size, source.length);
      assert.strictEqual((yield* second.status(id)).state, "completed");
      assert.strictEqual(fixture.sessions.size, 1);
      assert.deepStrictEqual(
        fixture.seen.filter((entry) => entry.method === "PATCH").map((entry) => entry.bytes),
        [6 * 1024 ** 2, 1024 ** 2],
      );
    }),
  ).pipe(Effect.provide(Layer.merge(FetchHttpClient.layer, KeyValueStore.layerMemory))),
);

it.effect("queries partial acceptance and completes an empty source", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* SupabaseStorage.make({
        url: fixture.url,
        bucket: "bucket",
        storeId: "test",
        auth: { mode: "user", token: Redacted.make("user") },
        resumable: true,
      });
      const endpoint = {
        storage,
        offsets: storage.offsets,
        maximumObjectBytes: policy.maxBytes,
        maximumSingleUploadBytes: policy.maxBytes,
        transportOverheadBytes: 1024,
      };
      const transfers = yield* makeTransfers(endpoint, policy);
      fixture.partial();
      fixture.losePatch();
      const result = yield* transfers.upload({
        key: "partial",
        source: UploadSource.bytes(new Uint8Array(1024).fill(3)),
        strategy: "offset",
      });
      assert.strictEqual(result.strategy, "offset");
      assert.strictEqual(result.progress.phase, "verified");
      const empty = yield* transfers.upload({
        key: "empty",
        source: UploadSource.bytes(new Uint8Array()),
        strategy: "offset",
      });
      assert.strictEqual(empty.metadata.size, 0);
    }),
  ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect("keeps expired and non-terminable tus sessions explicit", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      fixture.noTermination();
      const storage = yield* SupabaseStorage.make({
        url: fixture.url,
        bucket: "bucket",
        storeId: "test",
        auth: { mode: "user", token: Redacted.make("user") },
        resumable: true,
      });
      if (!storage.offsets) return assert.fail("Missing negotiated tus");
      const upload = yield* storage.offsets.create({ key: "pending", contentLength: 1 });
      const cancelled = yield* storage.offsets.cancel(upload).pipe(Effect.result);
      assert.strictEqual(
        cancelled._tag === "Failure" && cancelled.failure.reason._tag,
        "Unsupported",
      );
      assert.strictEqual(fixture.sessions.size, 1);
      fixture.expire();
      assert.strictEqual((yield* storage.offsets.query(upload))._tag, "Expired");
      yield* storage.offsets.cancel(upload);
      const forged = {
        ...upload,
        handle: Redacted.make(
          JSON.stringify({
            url: "https://untrusted.invalid/storage/v1/upload/resumable/other",
            total: 1,
          }),
        ),
      };
      const before = fixture.seen.length;
      assert.strictEqual(
        (yield* storage.offsets.query(forged).pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(fixture.seen.length, before);
    }),
  ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect("does not reconcile a completed tus URL to another writer's object", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* SupabaseStorage.make({
        url: fixture.url,
        bucket: "bucket",
        storeId: "test",
        auth: { mode: "user", token: Redacted.make("user") },
        resumable: true,
      });
      if (!storage.offsets) return assert.fail("Missing tus");
      const endpoint = {
        storage,
        offsets: storage.offsets,
        maximumObjectBytes: policy.maxBytes,
        maximumSingleUploadBytes: policy.maxBytes,
        transportOverheadBytes: 1024,
      };
      const store = yield* UploadSessionStore.make({
        namespace: "reconcile",
        ownership: "exclusive",
        protection: aes256Gcm(Redacted.make(new Uint8Array(32).fill(1))),
      });
      const source = UploadSource.bytes(new Uint8Array([7]));
      const first = yield* makeSessions(
        {
          ...endpoint,
          offsets: {
            ...storage.offsets,
            resolveCompletion: () =>
              Effect.fail(storageError("resolve", "supabase", { _tag: "ProviderFailure" })),
          },
        },
        store,
        { configurationId: "tus" },
        policy,
      );
      const id = yield* first.create({ key: "raced", source });
      assert.strictEqual((yield* first.resume(id, source).pipe(Effect.result))._tag, "Failure");
      fixture.objects.set("raced", {
        body: Buffer.from([7]),
        etag: '"replacement"',
        contentType: "application/octet-stream",
        metadata: { "effect-storage-operation": "another-writer" },
      });
      const restarted = yield* makeSessions(endpoint, store, { configurationId: "tus" }, policy);
      const result = yield* restarted.reconcile(id).pipe(Effect.result);
      assert.strictEqual(
        result._tag === "Failure" && result.failure.reason._tag,
        "IntegrityMismatch",
      );
      assert.strictEqual((yield* restarted.status(id)).state, "completing");
    }),
  ).pipe(Effect.provide(Layer.merge(FetchHttpClient.layer, KeyValueStore.layerMemory))),
);
