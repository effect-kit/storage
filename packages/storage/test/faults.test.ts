import { assert, it } from "@effect/vitest";
import { Cause, Effect, Exit, Fiber, Result, Stream } from "effect";
import { TestClock } from "effect/testing";

import { Storage } from "../src/index.js";
import { Faults, MemoryStorage } from "../src/testing.js";

it.effect("commit then response loss reports unknown and retains the object", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    const faults = yield* Faults.make(base, [
      { operation: "put", afterFailure: { _tag: "TransportFailure", code: "LostResponse" } },
    ]);
    const result = yield* Effect.result(
      faults.storage.put({
        key: "sensitive-key",
        body: new Uint8Array([1, 2]),
      }),
    );
    assert.isTrue(Result.isFailure(result) && result.failure.mutationOutcome === "unknown");
    assert.strictEqual((yield* base.head({ key: "sensitive-key" })).size, 2);
    yield* faults.assertDone;
    assert.notInclude(JSON.stringify(yield* faults.attempts), "sensitive-key");
    assert.strictEqual((yield* faults.attempts).at(0)?.status, "failed");
  }),
);

it.effect("before-send failure consumes no source and never becomes false existence", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    let reads = 0;
    const faults = yield* Faults.make(base, [
      { operation: "put", beforeFailure: { _tag: "AccessDenied" } },
      { operation: "head", beforeFailure: { _tag: "AccessDenied" } },
    ]);
    const result = yield* Effect.result(
      faults.storage.put({
        key: "denied",
        body: Stream.fromEffect(
          Effect.sync(() => {
            reads++;
            return new Uint8Array([1]);
          }),
        ),
      }),
    );
    assert.isTrue(Result.isFailure(result) && result.failure.mutationOutcome === "not-started");
    assert.strictEqual(reads, 0);
    const exists = yield* Effect.result(
      Storage.exists({ key: "denied" }).pipe(Effect.provideService(Storage, faults.storage)),
    );
    assert.isTrue(Result.isFailure(exists) && exists.failure.reason._tag === "AccessDenied");
    yield* faults.assertDone;
  }),
);

it.effect("mid-upload faults preserve the previously committed object", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    yield* base.put({ key: "original", body: new Uint8Array([9]) });
    const faults = yield* Faults.make(base, [
      {
        operation: "put",
        bodyFailure: { afterBytes: 1, reason: { _tag: "TransportFailure" } },
      },
    ]);
    const result = yield* Effect.result(
      faults.storage.put({
        key: "original",
        body: new Uint8Array([1, 2, 3]),
      }),
    );
    assert.isTrue(Result.isFailure(result) && result.failure.mutationOutcome === "unknown");

    assert.deepStrictEqual(
      yield* Storage.getBytes({ key: "original", maxBytes: 3 }).pipe(
        Effect.provideService(Storage, base),
      ),
      new Uint8Array([9]),
    );
  }),
);

it.effect("cleanup debt is observable without replacing the primary read failure", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    yield* base.put({ key: "read", body: new Uint8Array([1, 2, 3]) });
    const faults = yield* Faults.make(base, [
      {
        operation: "get",
        bodyFailure: { afterBytes: 1, reason: { _tag: "IntegrityMismatch" } },
        cleanupFailure: "CleanupUnavailable",
      },
    ]);
    const result = yield* Effect.result(
      Effect.scoped(
        Effect.gen(function* () {
          const response = yield* faults.storage.get({ key: "read" });
          if (response._tag === "Object") yield* Stream.runDrain(response.body);
        }),
      ),
    );
    assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "IntegrityMismatch");
    const diagnostics = yield* faults.diagnostics;
    assert.strictEqual(diagnostics.at(0)?.kind, "cleanup");
    assert.strictEqual(diagnostics.at(0)?.code, "CleanupUnavailable");
    assert.strictEqual((yield* faults.attempts).at(0)?.status, "failed");
  }),
);

it.effect("observer failure after commit does not turn success into rejection", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    const faults = yield* Faults.make(base, [
      { operation: "put", observerFailure: "ObserverFailed" },
    ]);
    const receipt = yield* faults.storage.put({ key: "observed", body: new Uint8Array([1]) });
    assert.strictEqual((yield* base.head({ ref: receipt.ref })).size, 1);
    assert.strictEqual((yield* faults.diagnostics).at(0)?.kind, "observer");
    assert.strictEqual((yield* faults.attempts).at(0)?.status, "succeeded");
  }),
);

it.effect("delays, throttling and interruption use Effect time without hidden retries", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    const faults = yield* Faults.make(base, [
      {
        operation: "head",
        delayMillis: 1000,
        beforeFailure: { _tag: "Throttled", retryAfterMillis: 500 },
      },
    ]);
    const fiber = yield* Effect.forkChild(Effect.result(faults.storage.head({ key: "delayed" })));
    yield* TestClock.adjust(1000);
    const result = yield* Fiber.join(fiber);
    assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "Throttled");
    assert.strictEqual((yield* faults.attempts).length, 1);
    yield* faults.assertDone;
    const pending = yield* Faults.make(base, [{ operation: "head", delayMillis: 60_000 }]);
    const waiting = yield* Effect.forkChild(pending.storage.head({ key: "interrupted" }));
    yield* Effect.yieldNow;
    yield* Fiber.interrupt(waiting);
    assert.strictEqual((yield* pending.attempts).at(0)?.status, "interrupted");
  }),
);

it.effect("bulk deletion keeps per-key rejected and unknown outcomes", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    for (const key of ["a", "b", "c"]) yield* base.put({ key, body: new Uint8Array() });
    const faults = yield* Faults.make(base, [
      { operation: "delete" },
      { operation: "delete", beforeFailure: { _tag: "AccessDenied" } },
      { operation: "delete", afterFailure: { _tag: "TransportFailure" } },
    ]);
    const outcomes = yield* Storage.deleteMany([{ key: "a" }, { key: "b" }, { key: "c" }], {
      concurrency: 1,
    }).pipe(Effect.provideService(Storage, faults.storage));
    const [a, b, c] = outcomes;
    assert.isTrue(a !== undefined && Result.isSuccess(a.result));
    assert.isTrue(
      b !== undefined &&
        Result.isFailure(b.result) &&
        b.result.failure.reason._tag === "AccessDenied",
    );
    assert.isTrue(
      c !== undefined &&
        Result.isFailure(c.result) &&
        c.result.failure.mutationOutcome === "unknown",
    );
    assert.isTrue(yield* Storage.exists({ key: "b" }).pipe(Effect.provideService(Storage, base)));
    assert.isFalse(yield* Storage.exists({ key: "c" }).pipe(Effect.provideService(Storage, base)));
  }),
);

it.effect("exhausted, mismatched and unused scripts fail loudly as test defects", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    const empty = yield* Faults.make(base, []);
    const exhausted = yield* Effect.exit(empty.storage.head({ key: "x" }));
    assert.isTrue(Exit.isFailure(exhausted) && Cause.hasDies(exhausted.cause));
    const mismatched = yield* Faults.make(base, [{ operation: "put" }]);
    const mismatch = yield* Effect.exit(mismatched.storage.head({ key: "x" }));
    assert.isTrue(Exit.isFailure(mismatch) && Cause.hasDies(mismatch.cause));
    const unused = yield* Faults.make(base, [{ operation: "get" }]);
    const notDone = yield* Effect.exit(unused.assertDone);
    assert.isTrue(Exit.isFailure(notDone) && Cause.hasDies(notDone.cause));
  }),
);
