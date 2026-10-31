import { assert, it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";

import { StorageCache } from "../../../src/composition/cache.js";
import { StoragePrefix, StorageReadOnly } from "../../../src/composition/views.js";
import { StorageSigning } from "../../../src/core/capabilities.js";
import { storageError } from "../../../src/core/errors.js";
import { Storage } from "../../../src/core/storage.js";
import { MemoryStorage } from "../../../src/testing/index.js";
import { TransferDriver } from "../../../src/transfers/dependencies.js";

const bytes = (text: string) => new TextEncoder().encode(text);

it.effect("prefixes map keys, identities, listings and bind continuation to the exact view", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const source = yield* MemoryStorage.make({ storeId: "source" });
      const scoped = yield* StoragePrefix.make({ prefix: "tenant-a/" }).pipe(
        Effect.provideService(Storage, source),
      );
      const other = yield* StoragePrefix.make({ prefix: "tenant-b/" }).pipe(
        Effect.provideService(Storage, source),
      );
      const receipt = yield* scoped.put({ key: "a", body: bytes("a") });
      yield* scoped.put({ key: "b", body: bytes("b") });
      yield* other.put({ key: "a", body: bytes("private") });
      assert.strictEqual(receipt.ref.key, "a");
      assert.notStrictEqual(receipt.ref.storeId, source.storeId);
      assert.strictEqual((yield* source.head({ key: "tenant-a/a" })).size, 1);
      assert.strictEqual((yield* scoped.head({ ref: receipt.ref })).size, 1);
      assert.strictEqual((yield* Effect.result(other.head({ ref: receipt.ref })))._tag, "Failure");
      const page = yield* scoped.listPage({ limit: 1 });
      assert.strictEqual(page.entries.length, 1);
      assert.isDefined(page.cursor);
      const second = yield* scoped.listPage({ limit: 1, cursor: page.cursor });
      assert.strictEqual(second.entries[0]?._tag, "Object");
      assert.strictEqual(
        (yield* Effect.result(other.listPage({ limit: 1, cursor: page.cursor })))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* Effect.result(
          scoped.listPage({ prefix: "different/", limit: 1, cursor: page.cursor }),
        ))._tag,
        "Failure",
      );
      const result = yield* scoped.get({ key: "a" });
      if (result._tag !== "Object") return assert.fail("Missing object");
      assert.strictEqual((yield* Stream.runCollect(result.body))[0]?.length, 1);
    }),
  ),
);

it.effect(
  "read-only layers reject mutations and expose no provider signing or native protocol bypass",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const base = MemoryStorage.layer();
        const context = yield* Layer.build(StorageReadOnly.layer.pipe(Layer.provide(base)));
        const storage = Context.get(context, Storage);
        assert.strictEqual(
          (yield* Effect.result(storage.put({ key: "x", body: bytes("x") })))._tag,
          "Failure",
        );
        assert.strictEqual((yield* Effect.result(storage.delete({ key: "x" })))._tag, "Failure");
        assert.isTrue(Option.isNone(Context.getOption(context, StorageSigning)));
        const endpoint = Context.get(context, TransferDriver);
        assert.strictEqual(endpoint.storage, storage);
        assert.isUndefined(endpoint.multipart);
        assert.isUndefined(endpoint.nativeCopy);
      }),
    ),
);

it.effect(
  "native Effect caches coalesce metadata, expire entries and invalidate after writes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const source = yield* MemoryStorage.make();
        yield* source.put({ key: "x", body: bytes("old") });
        let calls = 0;
        const counted = Storage.make({
          ...source,
          head: (request) =>
            Effect.sync(() => {
              calls++;
            }).pipe(Effect.andThen(source.head(request))),
        });
        const cached = yield* StorageCache.make({ capacity: 4, timeToLive: "1 second" }).pipe(
          Effect.provideService(Storage, counted),
        );
        yield* Effect.all([cached.head({ key: "x" }), cached.head({ key: "x" })], {
          concurrency: "unbounded",
        });
        assert.strictEqual(calls, 1);
        yield* TestClock.adjust("2 seconds");
        yield* cached.head({ key: "x" });
        assert.strictEqual(calls, 2);
        yield* cached.put({ key: "x", body: bytes("new-value") });
        assert.strictEqual((yield* cached.head({ key: "x" })).size, 9);
        assert.strictEqual(calls, 3);
      }),
    ),
);

it.effect(
  "failed lookups are not retained and uncertain writes invalidate successful metadata",
  () =>
    Effect.gen(function* () {
      const source = yield* MemoryStorage.make();
      let denied = true;
      const unstable = Storage.make({
        ...source,
        head: (request) =>
          denied
            ? Effect.fail(storageError("head", "test", { _tag: "AccessDenied" }))
            : source.head(request),
        put: (request) =>
          source
            .put(request)
            .pipe(
              Effect.andThen(
                Effect.fail(storageError("put", "test", { _tag: "TransportFailure" }, "unknown")),
              ),
            ),
      });
      const cached = yield* StorageCache.make({ capacity: 4, timeToLive: "1 hour" }).pipe(
        Effect.provideService(Storage, unstable),
      );
      yield* source.put({ key: "x", body: bytes("a") });
      assert.strictEqual((yield* Effect.result(cached.head({ key: "x" })))._tag, "Failure");
      denied = false;
      assert.strictEqual((yield* cached.head({ key: "x" })).size, 1);
      yield* Effect.result(cached.put({ key: "x", body: bytes("changed") }));
      assert.strictEqual((yield* cached.head({ key: "x" })).size, 7);
    }),
);

it.effect("object cache returns fresh byte streams and preserves metadata timestamps", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const source = yield* MemoryStorage.make();
      yield* source.put({ key: "x", body: bytes("hello") });
      let reads = 0;
      const counted = Storage.make({
        ...source,
        get: (request) =>
          Effect.sync(() => {
            reads++;
          }).pipe(Effect.andThen(source.get(request))),
      });
      const cached = yield* StorageCache.make({
        capacity: 4,
        timeToLive: "1 minute",
        objects: { capacity: 2, maxObjectBytes: 8 },
      }).pipe(Effect.provideService(Storage, counted));
      const first = yield* cached.get({ key: "x" });
      if (first._tag !== "Object") return assert.fail("Missing object");
      const collected = yield* Stream.runCollect(first.body);
      if (collected[0]) collected[0][0] = 0;
      const second = yield* cached.get({ key: "x" });
      if (second._tag !== "Object") return assert.fail("Missing object");
      assert.strictEqual(
        new TextDecoder().decode((yield* Stream.runCollect(second.body))[0]),
        "hello",
      );
      assert.strictEqual(reads, 1);
    }),
  ),
);

it.effect("byte cache validates external versions and emits bounded fresh chunks", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const source = yield* MemoryStorage.make();
      yield* source.put({ key: "x", body: new Uint8Array(150000).fill(1) });
      const cached = yield* StorageCache.make({
        capacity: 4,
        timeToLive: "1 hour",
        objects: { capacity: 2, maxObjectBytes: 200000 },
      }).pipe(Effect.provideService(Storage, source));
      const first = yield* cached.get({ key: "x" });
      if (first._tag !== "Object") return assert.fail("Missing object");
      const chunks = yield* Stream.runCollect(first.body);
      assert.isTrue(chunks.every((chunk) => chunk.length <= 65536));
      yield* source.put({ key: "x", body: bytes("external change") });
      const changed = yield* cached.get({ key: "x" });
      if (changed._tag !== "Object") return assert.fail("Missing changed object");
      assert.strictEqual(
        new TextDecoder().decode((yield* Stream.runCollect(changed.body))[0]),
        "external change",
      );
    }),
  ),
);

it.effect("observed access denial evicts protected metadata and bytes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const source = yield* MemoryStorage.make();
      yield* source.put({ key: "x", body: bytes("private") });
      let denied = false;
      const guarded = Storage.make({
        ...source,
        head: (request) =>
          denied
            ? Effect.fail(storageError("head", "test", { _tag: "AccessDenied" }))
            : source.head(request),
      });
      const cached = yield* StorageCache.make({
        capacity: 4,
        timeToLive: "1 hour",
        objects: { capacity: 2, maxObjectBytes: 100 },
      }).pipe(Effect.provideService(Storage, guarded));
      yield* cached.head({ key: "x" });
      const first = yield* cached.get({ key: "x" });
      if (first._tag === "Object") yield* Stream.runDrain(first.body);
      denied = true;
      assert.strictEqual((yield* Effect.result(cached.get({ key: "x" })))._tag, "Failure");
      assert.strictEqual((yield* Effect.result(cached.head({ key: "x" })))._tag, "Failure");
    }),
  ),
);

it.live("cancelled byte-cache fills release their producer and cannot become cache hits", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const source = yield* MemoryStorage.make();
      yield* source.put({ key: "x", body: bytes("hello") });
      const started = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      let blocking = true;
      let reads = 0;
      const guarded = Storage.make({
        ...source,
        get: (request) =>
          source.get(request).pipe(
            Effect.map((result) => {
              reads++;
              if (result._tag !== "Object" || !blocking) return result;
              return {
                ...result,
                body: Stream.concat(result.body, Stream.never).pipe(
                  Stream.tap(() => Deferred.succeed(started, undefined)),
                  Stream.ensuring(Deferred.succeed(released, undefined)),
                ),
              };
            }),
          ),
      });
      const cached = yield* StorageCache.make({
        capacity: 4,
        timeToLive: "1 minute",
        objects: { capacity: 2, maxObjectBytes: 100 },
      }).pipe(Effect.provideService(Storage, guarded));
      const fiber = yield* Effect.forkChild(cached.get({ key: "x" }));
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      yield* Deferred.await(released).pipe(Effect.timeout("2 seconds"));
      blocking = false;
      const result = yield* cached.get({ key: "x" });
      if (result._tag !== "Object") return assert.fail("Missing object");
      assert.strictEqual(
        new TextDecoder().decode((yield* Stream.runCollect(result.body))[0]),
        "hello",
      );
      assert.strictEqual(reads, 2);
    }),
  ),
);
