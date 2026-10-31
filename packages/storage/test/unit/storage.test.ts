import { assert, it } from "@effect/vitest";
import { Effect, Ref, Result, Stream } from "effect";

import { Storage } from "../../src/index.js";
import { MemoryStorage } from "../../src/testing/index.js";

it.effect("unknown-size collection stops producers at the byte limit and closes the response", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    let pulls = 0;
    let released = 0;
    const storage = Storage.make({
      ...base,
      get: Effect.fnUntraced(function* () {
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            released++;
          }),
        );
        return {
          _tag: "Object" as const,
          metadata: { ref: { storeId: base.storeId, key: "unknown" } },
          body: Stream.fromIterable([new Uint8Array(2), new Uint8Array(2), new Uint8Array(2)], {
            chunkSize: 1,
          }).pipe(
            Stream.tap(() =>
              Effect.sync(() => {
                pulls++;
              }),
            ),
          ),
        };
      }),
    });
    const result = yield* Effect.result(
      Storage.getBytes({ key: "unknown", maxBytes: 3 }).pipe(
        Effect.provideService(Storage, storage),
      ),
    );
    assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "LimitExceeded");
    assert.strictEqual(pulls, 2);
    assert.strictEqual(released, 1);
  }),
);

it.effect("collection rejects false lengths and invalid UTF-8 without fabricating bytes", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    yield* base.put({ key: "length", body: new Uint8Array([255]) });
    const wrongLength = Storage.make({
      ...base,
      get: (request) =>
        base
          .get(request)
          .pipe(
            Effect.map((response) =>
              response._tag === "Object" ? { ...response, contentLength: 0 } : response,
            ),
          ),
    });
    const mismatch = yield* Effect.result(
      Storage.getBytes({ key: "length", maxBytes: 2 }).pipe(
        Effect.provideService(Storage, wrongLength),
      ),
    );
    assert.isTrue(Result.isFailure(mismatch) && mismatch.failure.reason._tag === "LengthMismatch");
    const invalid = yield* Effect.result(
      Storage.getText({ key: "length", maxBytes: 2 }).pipe(Effect.provideService(Storage, base)),
    );
    assert.isTrue(Result.isFailure(invalid) && invalid.failure.reason._tag === "InvalidInput");
  }),
);

it.effect("a scoped response body is single-use and cannot be consumed after closure", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make();
    yield* storage.put({ key: "body", body: new Uint8Array([1]) });
    const response = yield* Effect.scoped(
      Effect.gen(function* () {
        const acquired = yield* storage.get({ key: "body" });
        if (acquired._tag === "Object") {
          yield* Stream.runDrain(acquired.body);
          const repeated = yield* Effect.result(Stream.runDrain(acquired.body));
          assert.isTrue(
            Result.isFailure(repeated) && repeated.failure.reason._tag === "BodyConsumed",
          );
        }
        return acquired;
      }),
    );
    if (response._tag === "Object") {
      const closed = yield* Effect.result(Effect.scoped(Stream.runDrain(response.body)));
      assert.isTrue(Result.isFailure(closed) && closed.failure.reason._tag === "ScopeClosed");
    }
  }),
);

it.effect("listing is lazy and handles an empty page with continuation", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    yield* base.put({ key: "visible", body: new Uint8Array() });
    const requests = yield* Ref.make(0);
    const storage = Storage.make({
      ...base,
      listPage: Effect.fnUntraced(function* () {
        const count = yield* Ref.getAndUpdate(requests, (current) => current + 1);
        if (count === 0) return { entries: [], cursor: "empty-page" };
        return yield* base.listPage();
      }),
    });
    const stream = Storage.list().pipe(Stream.provideService(Storage, storage));
    assert.strictEqual(yield* Ref.get(requests), 0);
    const entries = yield* Stream.runCollect(stream);
    assert.strictEqual(entries.length, 1);
    assert.strictEqual(yield* Ref.get(requests), 2);
  }),
);

it.effect("stopping a list consumer stops requests and repeated cursors fail explicitly", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    yield* base.put({ key: "a", body: new Uint8Array() });
    yield* base.put({ key: "b", body: new Uint8Array() });
    let calls = 0;
    const storage = Storage.make({
      ...base,
      listPage: (request) =>
        Effect.suspend(() => {
          calls++;
          return base.listPage(request);
        }),
    });
    yield* Stream.runCollect(
      Storage.list({ limit: 1 }).pipe(Stream.take(1), Stream.provideService(Storage, storage)),
    );
    assert.strictEqual(calls, 1);
    const invalid = Storage.make({
      ...base,
      listPage: () => Effect.succeed({ entries: [], cursor: "same" }),
    });
    const result = yield* Effect.result(
      Stream.runCollect(Storage.list().pipe(Stream.provideService(Storage, invalid))),
    );
    assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "ProviderFailure");
  }),
);

it.effect("cursors reject changed namespace, prefix, delimiter, limit and corrupt data", () =>
  Effect.gen(function* () {
    const a = yield* MemoryStorage.make({ storeId: "a" });
    const b = yield* MemoryStorage.make({ storeId: "b" });
    const freshA = yield* MemoryStorage.make({ storeId: "a" });
    for (const key of ["p/a", "p/b"]) yield* a.put({ key, body: new Uint8Array() });
    const page = yield* a.listPage({ prefix: "p/", limit: 1 });
    assert.isDefined(page.cursor);
    for (const request of [
      { prefix: "different", limit: 1, cursor: page.cursor },
      { prefix: "p/", delimiter: "/", limit: 1, cursor: page.cursor },
      { prefix: "p/", limit: 2, cursor: page.cursor },
      { cursor: "not-a-valid-cursor" },
    ]) {
      const result = yield* Effect.result(a.listPage(request));
      assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "InvalidInput");
    }
    const foreign = yield* Effect.result(
      b.listPage({ prefix: "p/", limit: 1, cursor: page.cursor }),
    );
    assert.isTrue(Result.isFailure(foreign) && foreign.failure.reason._tag === "InvalidInput");

    const previousInstance = yield* Effect.result(
      freshA.listPage({ prefix: "p/", limit: 1, cursor: page.cursor }),
    );
    assert.isTrue(
      Result.isFailure(previousInstance) && previousInstance.failure.reason._tag === "InvalidInput",
    );
  }),
);

it.effect(
  "unsupported conditions fail before source consumption and capabilities are immutable",
  () =>
    Effect.gen(function* () {
      const base = yield* MemoryStorage.make();
      let consumed = false;
      const storage = Storage.make({
        ...base,
        capabilities: { ...base.capabilities, putConditions: [] },
      });
      const rejected = yield* Effect.result(
        storage.put({
          key: "unsupported",
          condition: { _tag: "CreateOnly" },
          body: Stream.fromEffect(
            Effect.sync(() => {
              consumed = true;
              return new Uint8Array();
            }),
          ),
        }),
      );
      assert.isTrue(Result.isFailure(rejected) && rejected.failure.reason._tag === "Unsupported");
      assert.isFalse(consumed);
      assert.isTrue(Object.isFrozen(storage.capabilities.putConditions));
    }),
);
