import { assert, it } from "@effect/vitest";
import { Cause, Effect, Exit, Fiber, Result, Stream } from "effect";
import { TestClock } from "effect/testing";

import { storageError } from "../../src/errors.js";
import { UploadSource } from "../../src/transfers.js";
import { fixture, policy, makeTransfers } from "./fixture.js";

it.effect("joins interrupted part workers before abort and releases the shared workspace", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    let active = 0;
    let activeAtAbort = -1;
    const transfers = yield* makeTransfers(
      {
        ...test.endpoint,
        multipart: {
          ...test.multipart,
          uploadPart: () =>
            Effect.acquireUseRelease(
              Effect.sync(() => {
                active++;
              }),
              () => Effect.never,
              () =>
                Effect.sync(() => {
                  active--;
                }),
            ),
          abort: () =>
            Effect.sync(() => {
              activeAtAbort = active;
            }),
        },
      },
      policy,
    );
    const fiber = yield* transfers
      .upload({ key: "interrupted", source: UploadSource.bytes(new Uint8Array(64)) })
      .pipe(Effect.forkChild);
    yield* TestClock.adjust(1);
    assert.strictEqual(active, 2);
    yield* Fiber.interrupt(fiber);
    assert.strictEqual(activeAtAbort, 0);
    assert.strictEqual((yield* transfers.resources).reservedBytes, 0);
    assert.strictEqual((yield* transfers.resources).activeRequests, 0);
  }),
);

it.effect("bounds a stuck abort separately from the total transfer deadline", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const transfers = yield* makeTransfers(
      {
        ...test.endpoint,
        multipart: { ...test.multipart, uploadPart: () => Effect.never, abort: () => Effect.never },
      },
      { ...policy, deadlineMillis: 10, cleanupTimeoutMillis: 5 },
    );
    const fiber = yield* transfers
      .upload({ key: "deadline", source: UploadSource.bytes(new Uint8Array(32)) })
      .pipe(Effect.forkChild);
    yield* TestClock.adjust(15);
    const exit = yield* Fiber.await(fiber);
    assert.isTrue(Exit.isFailure(exit) && Cause.hasFails(exit.cause));
    assert.strictEqual((yield* transfers.resources).reservedBytes, 0);
    assert.strictEqual((yield* transfers.diagnostics).entries[0]?.kind, "cleanup");
  }),
);

it.effect("enforces one attempt budget across concurrent workers and honors Retry-After", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    let calls = 0;
    const transfers = yield* makeTransfers(
      {
        ...test.endpoint,
        multipart: {
          ...test.multipart,
          uploadPart: (request) => {
            calls++;
            return calls === 1
              ? Effect.fail(
                  storageError(
                    "part",
                    "test",
                    { _tag: "Throttled", retryAfterMillis: 100 },
                    "rejected",
                  ),
                )
              : test.multipart.uploadPart(request);
          },
        },
      },
      { ...policy, partConcurrency: 1, maxAttempts: 4 },
    );
    const fiber = yield* transfers
      .upload({ key: "budget", source: UploadSource.bytes(new Uint8Array(32)) })
      .pipe(Effect.forkChild);
    yield* TestClock.adjust(99);
    assert.strictEqual(calls, 1);
    yield* TestClock.adjust(1);
    const result = yield* Fiber.join(fiber).pipe(Effect.result);
    assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "LimitExceeded");
    assert.strictEqual(calls, 3);
    assert.strictEqual(test.counts().completes, 0);
    assert.strictEqual(test.counts().aborts, 1);
  }),
);

it.effect("shares admission and request limits across uneven uploads without starvation", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const transfers = yield* makeTransfers(
      {
        ...test.endpoint,
        multipart: {
          ...test.multipart,
          uploadPart: (request) =>
            test.multipart
              .uploadPart(request)
              .pipe(Effect.delay(request.partNumber === 1 ? 20 : 1)),
        },
      },
      { ...policy, memoryBudgetBytes: 70000, requestConcurrency: 2 },
    );
    const fiber = yield* Effect.forEach(
      [0, 1, 2, 3, 4],
      (index) =>
        transfers.upload({
          key: `fair-${index}`,
          source: UploadSource.stream(
            Stream.fromIterable(
              Array.from({ length: 8 }, () => new Uint8Array(8)),
              { chunkSize: 1 },
            ).pipe(Stream.tap(() => Effect.sleep(1))),
            64,
          ),
        }),
      { concurrency: 5 },
    ).pipe(Effect.forkChild);
    yield* TestClock.adjust(500);
    const results = yield* Fiber.join(fiber);
    assert.strictEqual(results.length, 5);
    const resources = yield* transfers.resources;
    assert.isAtMost(resources.peakReservedBytes, 70000);
    assert.strictEqual(resources.peakActiveRequests, 2);
    assert.strictEqual(resources.reservedBytes, 0);
    assert.strictEqual(resources.activeRequests, 0);
  }),
);

it.effect("leaves ambiguous completion for reconciliation rather than aborting or restarting", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const transfers = yield* makeTransfers(
      {
        ...test.endpoint,
        reconcile: () => Effect.succeed(undefined),
        multipart: { ...test.multipart, complete: () => Effect.never },
      },
      { ...policy, deadlineMillis: 10 },
    );
    const fiber = yield* transfers
      .upload({ key: "unknown", source: UploadSource.bytes(new Uint8Array(32)) })
      .pipe(Effect.forkChild);
    yield* TestClock.adjust(10);
    const result = yield* Fiber.join(fiber).pipe(Effect.result);
    assert.isTrue(Result.isFailure(result) && result.failure.mutationOutcome === "unknown");
    assert.strictEqual(test.counts().initiates, 1);
    assert.strictEqual(test.counts().aborts, 0);
    const diagnostic = (yield* transfers.diagnostics).entries[0];
    assert.strictEqual(diagnostic?.kind, "unknown-outcome");
    assert.isDefined(diagnostic?.upload);
  }),
);

it.effect("a hanging final observer cannot turn a confirmed commit into a deadline error", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const transfers = yield* makeTransfers(test.endpoint, {
      ...policy,
      deadlineMillis: 10,
      observerTimeoutMillis: 20,
      observer: (event) => (event.phase === "committed" ? Effect.never : Effect.void),
    });
    const fiber = yield* transfers
      .upload({ key: "observer", source: UploadSource.bytes(new Uint8Array(1)) })
      .pipe(Effect.forkChild);
    yield* TestClock.adjust(30);
    const receipt = yield* Fiber.join(fiber);
    assert.strictEqual(receipt.progress.bytesAcknowledged, 1);
    assert.strictEqual((yield* transfers.diagnostics).entries[0]?.kind, "observer");
  }),
);
