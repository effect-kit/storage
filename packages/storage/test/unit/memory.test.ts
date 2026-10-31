import { assert, it } from "@effect/vitest";
import { Context, DateTime, Deferred, Effect, Fiber, Layer, Result, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";

import { Storage } from "../../src/index.js";
import { MemoryStorage } from "../../src/testing/index.js";

class SourceFailure extends Schema.TaggedError<SourceFailure>()("SourceFailure", {}) {}

const gatedSource = (
  started: Deferred.Deferred<void>,
  gate: Deferred.Deferred<void>,
  byte: number,
) =>
  Stream.fromEffect(
    Effect.gen(function* () {
      yield* Deferred.succeed(started, undefined);
      yield* Deferred.await(gate);
      return new Uint8Array([byte]);
    }),
  );

it.effect("source failures preserve their identity and never publish staged bytes", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make();
    const failure = new SourceFailure();
    const result = yield* Effect.result(
      storage.put({
        key: "failed",
        body: Stream.concat(Stream.succeed(new Uint8Array([1])), Stream.fail(failure)),
      }),
    );
    assert.isTrue(Result.isFailure(result) && result.failure === failure);
    assert.isFalse(
      yield* Storage.exists({ key: "failed" }).pipe(Effect.provideService(Storage, storage)),
    );
  }),
);

it.effect("conditions are evaluated when racing streaming writers commit", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make();
    const startedA = yield* Deferred.make<void>();
    const startedB = yield* Deferred.make<void>();
    const gateA = yield* Deferred.make<void>();
    const gateB = yield* Deferred.make<void>();
    const writerA = yield* Effect.forkChild(
      Effect.result(
        storage.put({
          key: "race",
          body: gatedSource(startedA, gateA, 1),
          condition: { _tag: "CreateOnly" },
        }),
      ),
    );
    const writerB = yield* Effect.forkChild(
      Effect.result(
        storage.put({
          key: "race",
          body: gatedSource(startedB, gateB, 2),
          condition: { _tag: "CreateOnly" },
        }),
      ),
    );
    yield* Deferred.await(startedA);
    yield* Deferred.await(startedB);
    yield* Deferred.succeed(gateB, undefined);
    assert.isTrue(Result.isSuccess(yield* Fiber.join(writerB)));
    yield* Deferred.succeed(gateA, undefined);
    const loser = yield* Fiber.join(writerA);
    assert.isTrue(Result.isFailure(loser) && loser.failure.reason._tag === "PreconditionFailed");
    const bytes = yield* Storage.getBytes({ key: "race", maxBytes: 1 }).pipe(
      Effect.provideService(Storage, storage),
    );
    assert.deepStrictEqual(bytes, new Uint8Array([2]));
  }),
);

it.effect("interruption releases staging quota without publishing partial content", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make({ maxPendingBytes: 4 });
    const staged = yield* Deferred.make<void>();
    const waiting = yield* Deferred.make<Uint8Array>();
    const source = Stream.concat(
      Stream.succeed(new Uint8Array([1, 2])),
      Stream.fromEffect(
        Deferred.succeed(staged, undefined).pipe(Effect.andThen(Deferred.await(waiting))),
      ),
    );
    const fiber = yield* Effect.forkChild(storage.put({ key: "pending", body: source }));
    yield* Deferred.await(staged);
    const quota = yield* Effect.result(storage.put({ key: "other", body: new Uint8Array([3]) }));
    assert.isTrue(Result.isFailure(quota) && quota.failure.reason._tag === "QuotaExceeded");
    yield* Fiber.interrupt(fiber);
    yield* storage.put({ key: "after", body: new Uint8Array([4, 5]) });
    assert.isFalse(
      yield* Storage.exists({ key: "pending" }).pipe(Effect.provideService(Storage, storage)),
    );
  }),
);

it.effect("input, response buffers and metadata cannot mutate committed state", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* MemoryStorage.make({ chunkSize: 1 });
      const bytes = Buffer.from([1, 2]);
      const customMetadata = { label: "original" };
      const saved = yield* storage.put({ key: "owned", body: bytes, customMetadata });
      bytes[0] = 9;
      customMetadata.label = "changed";
      const response = yield* storage.get({ key: "owned" });
      assert.strictEqual(response._tag, "Object");
      if (response._tag !== "Object") return;
      yield* Stream.runForEach(response.body, (chunk) =>
        Effect.sync(() => {
          chunk[0] = 8;
        }),
      );
      Object.assign(saved.customMetadata ?? {}, { label: "response mutation" });
      const head = yield* storage.head({ key: "owned" });
      assert.strictEqual(head.customMetadata?.label, "original");
      assert.deepStrictEqual(
        yield* Storage.getBytes({ key: "owned", maxBytes: 2 }).pipe(
          Effect.provideService(Storage, storage),
        ),
        new Uint8Array([1, 2]),
      );
    }),
  ),
);

it.effect("length, actual byte, object count and retained-byte limits are enforced", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make({
      maxObjectBytes: 3,
      maxTotalBytes: 4,
      maxObjects: 2,
    });
    const short = yield* Effect.result(
      storage.put({
        key: "short",
        contentLength: 3,
        body: Stream.succeed(new Uint8Array([1, 2])),
      }),
    );
    assert.isTrue(Result.isFailure(short) && short.failure.reason._tag === "LengthMismatch");
    const oversized = yield* Effect.result(storage.put({ key: "large", body: new Uint8Array(4) }));
    assert.isTrue(Result.isFailure(oversized) && oversized.failure.reason._tag === "LimitExceeded");
    yield* storage.put({ key: "a", body: new Uint8Array(3) });
    const full = yield* Effect.result(storage.put({ key: "b", body: new Uint8Array(2) }));
    assert.isTrue(Result.isFailure(full) && full.failure.reason._tag === "QuotaExceeded");
    yield* storage.put({ key: "a", body: new Uint8Array(1) });
    yield* storage.put({ key: "b", body: new Uint8Array(1) });
    const count = yield* Effect.result(storage.put({ key: "c", body: new Uint8Array() }));
    assert.isTrue(Result.isFailure(count) && count.failure.reason._tag === "QuotaExceeded");
  }),
);

it.effect("open downloads reserve and release pinned read quota", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make({ maxReadBytes: 2, chunkSize: 1 });
    yield* storage.put({ key: "read", body: new Uint8Array(2) });
    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* storage.get({ key: "read" });
        const rejected = yield* Effect.result(storage.get({ key: "read" }));
        assert.isTrue(
          Result.isFailure(rejected) && rejected.failure.reason._tag === "QuotaExceeded",
        );
      }),
    );
    yield* Effect.scoped(
      Effect.gen(function* () {
        const response = yield* storage.get({ key: "read" });
        if (response._tag === "Object") yield* Stream.runDrain(response.body.pipe(Stream.take(1)));
      }),
    );

    yield* Storage.getBytes({ key: "read", maxBytes: 2 }).pipe(
      Effect.provideService(Storage, storage),
    );
  }),
);

it.effect("a download pins matching bytes and metadata across concurrent replacement", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* MemoryStorage.make();
      const first = yield* storage.put({ key: "snapshot", body: new Uint8Array([1, 2]) });
      const response = yield* storage.get({ key: "snapshot" });
      yield* storage.put({ key: "snapshot", body: new Uint8Array([3]) });
      assert.strictEqual(response.metadata.etag, first.etag);
      assert.strictEqual(response.metadata.size, 2);
      if (response._tag !== "Object") return;
      const chunks = yield* Stream.runCollect(response.body);
      assert.deepStrictEqual(
        Array.from(chunks).flatMap((chunk) => Array.from(chunk)),
        [1, 2],
      );
      assert.notStrictEqual((yield* storage.head({ key: "snapshot" })).etag, first.etag);
    }),
  ),
);

it.effect("timestamps use the test clock and pinned current refs reject replacement", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make();
    const original = yield* storage.put({ key: "version", body: new Uint8Array([1]) });
    yield* TestClock.adjust(1000);
    const replacement = yield* storage.put({ key: "version", body: new Uint8Array([2]) });
    assert.isDefined(original.lastModified);
    assert.isDefined(replacement.lastModified);
    if (original.lastModified && replacement.lastModified) {
      assert.strictEqual(
        DateTime.toEpochMillis(replacement.lastModified) -
          DateTime.toEpochMillis(original.lastModified),
        1000,
      );
    }
    const stale = yield* Effect.result(storage.head({ ref: original.ref }));
    assert.isTrue(Result.isFailure(stale) && stale.failure.reason._tag === "NotFound");
    yield* storage.delete({ ref: original.ref });
    assert.strictEqual((yield* storage.head({ key: "version" })).etag, replacement.etag);
  }),
);

it.effect("fresh Layers isolate state and deliberate Layer reuse shares state", () =>
  Effect.gen(function* () {
    const first = MemoryStorage.layer();
    const second = MemoryStorage.layer();
    const contexts = yield* Effect.scoped(
      Effect.gen(function* () {
        const a = yield* Layer.build(first);
        const b = yield* Layer.build(second);
        return { a, b };
      }),
    );
    const a = Context.get(contexts.a, Storage);
    const b = Context.get(contexts.b, Storage);
    assert.notStrictEqual(a.storeId, b.storeId);
    yield* a.put({ key: "isolated", body: new Uint8Array([1]) });
    const missing = yield* Effect.result(b.head({ key: "isolated" }));
    assert.isTrue(Result.isFailure(missing) && missing.failure.reason._tag === "NotFound");
    const shared = yield* Storage.exists({ key: "shared" }).pipe(
      Effect.tap(() =>
        Storage.use((storage) => storage.put({ key: "shared", body: new Uint8Array() })),
      ),
      Effect.andThen(Storage.exists({ key: "shared" })),
      Effect.provide(first),
    );
    assert.isTrue(shared);
  }),
);
