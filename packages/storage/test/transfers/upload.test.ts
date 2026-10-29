import { assert, it } from "@effect/vitest";
import { Effect, Result, Stream } from "effect";

import { storageError } from "../../src/errors.js";
import { UploadSource } from "../../src/transfers.js";
import { fixture, policy, makeTransfers } from "./fixture.js";

it.effect(
  "uploads immutable bytes, zero bytes and unknown-length multipart with bounded reservations",
  () =>
    Effect.gen(function* () {
      const test = yield* fixture();
      const transfers = yield* makeTransfers(test.endpoint, policy);
      const input = new Uint8Array([1, 2]);
      const source = UploadSource.bytes(input);
      input[0] = 99;
      const single = yield* transfers.upload({ key: "single", source });
      assert.strictEqual(single.strategy, "single");
      const empty = yield* transfers.upload({
        key: "empty",
        source: UploadSource.bytes(new Uint8Array()),
      });
      assert.strictEqual(empty.progress.bytesAcknowledged, 0);
      let opens = 0;
      const result = yield* transfers.upload({
        key: "large",
        source: UploadSource.stream(
          Stream.suspend(() => {
            opens++;
            return Stream.fromIterable(
              Array.from({ length: 20 }, () => new Uint8Array(8)),
              { chunkSize: 1 },
            );
          }),
        ),
      });
      assert.strictEqual(result.strategy, "multipart");
      assert.strictEqual(result.progress.bytesRead, 160);
      assert.strictEqual(result.progress.bytesAcknowledged, 160);
      assert.strictEqual(opens, 1);
      assert.strictEqual(test.counts().partAttempts, 10);
      assert.strictEqual(test.counts().aborts, 0);
      assert.strictEqual((yield* test.endpoint.storage.head({ key: "single" })).size, 2);
      const resources = yield* transfers.resources;
      assert.strictEqual(resources.reservedBytes, 0);
      assert.isAtMost(resources.peakReservedBytes, policy.memoryBudgetBytes ?? 0);
    }),
);

it.effect("retains and retries parts without reopening a one-shot source", () =>
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
                    { _tag: "Throttled", retryAfterMillis: 0 },
                    "rejected",
                  ),
                )
              : test.multipart.uploadPart(request);
          },
        },
      },
      policy,
    );
    const source = UploadSource.stream(Stream.succeed(new Uint8Array(32)), 32);
    const result = yield* transfers.upload({ key: "retry", source });
    assert.strictEqual(result.progress.bytesRead, 32);
    assert.strictEqual(result.progress.bytesSent, 48);
    assert.strictEqual(result.progress.attempts, 5);
    const consumed = yield* Effect.result(transfers.upload({ key: "again", source }));
    assert.isTrue(Result.isFailure(consumed) && consumed.failure.reason._tag === "BodyConsumed");
  }),
);

it.effect("keeps source failure and cleanup debt and never completes a partial object", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const failure = { _tag: "SourceFailure" } as const;
    const transfers = yield* makeTransfers(
      {
        ...test.endpoint,
        multipart: {
          ...test.multipart,
          abort: () =>
            Effect.fail(storageError("abort", "test", { _tag: "AccessDenied" }, "rejected")),
        },
      },
      policy,
    );
    const result = yield* Effect.result(
      transfers.upload({
        key: "broken",
        source: UploadSource.stream(
          Stream.concat(Stream.succeed(new Uint8Array(16)), Stream.fail(failure)),
        ),
      }),
    );
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result)) assert.strictEqual(result.failure, failure);
    assert.strictEqual(test.counts().completes, 0);
    assert.strictEqual((yield* transfers.diagnostics).entries[0]?.kind, "cleanup");
  }),
);

it.effect("reconciles a lost completion receipt and isolates a failing observer after commit", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const transfers = yield* makeTransfers(
      {
        ...test.endpoint,
        multipart: {
          ...test.multipart,
          complete: (request) =>
            test.multipart
              .complete(request)
              .pipe(
                Effect.andThen(
                  Effect.fail(
                    storageError("complete", "test", { _tag: "TransportFailure" }, "unknown"),
                  ),
                ),
              ),
        },
      },
      { ...policy, observer: () => Effect.die("observer defect") },
    );
    const receipt = yield* transfers.upload({
      key: "committed",
      source: UploadSource.bytes(new Uint8Array(32)),
    });
    assert.strictEqual(receipt.progress.bytesAcknowledged, 32);
    assert.strictEqual(test.counts().completes, 1);
    assert.strictEqual(test.counts().aborts, 0);
    assert.isTrue(
      (yield* transfers.diagnostics).entries.every((entry) => entry.kind === "observer"),
    );
  }),
);

it.effect(
  "rejects impossible plans, hidden backing buffers, false lengths and checksum mismatch",
  () =>
    Effect.gen(function* () {
      const test = yield* fixture();
      const transfers = yield* makeTransfers(test.endpoint, policy);
      const backing = new Uint8Array(1024);
      for (const request of [
        { source: UploadSource.stream(Stream.succeed(backing.subarray(0, 1)), 1) },
        { source: UploadSource.stream(Stream.succeed(new Uint8Array(3)), 2) },
        { source: UploadSource.stream(Stream.succeed(new Uint8Array(1)), 2) },
        { source: UploadSource.bytes(new Uint8Array(513)) },
        {
          source: UploadSource.bytes(new Uint8Array(1)),
          expectedChecksum: {
            algorithm: "sha256",
            encoding: "hex",
            scope: "full-object",
            value: "bad",
          } as const,
        },
      ])
        assert.isTrue(
          Result.isFailure(yield* Effect.result(transfers.upload({ key: "invalid", ...request }))),
        );
      assert.deepEqual(test.counts(), { aborts: 0, initiates: 0, completes: 0, partAttempts: 0 });
    }),
);

it.effect("bounds observer diagnostics while keeping every committed batch outcome", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const transfers = yield* makeTransfers(test.endpoint, {
      ...policy,
      observer: () => Effect.fail("observer failure"),
    });
    const requests = Stream.fromIterable(
      Array.from({ length: 200 }, (_, index) => ({
        key: `observed-${index}`,
        source: UploadSource.bytes(new Uint8Array(1)),
      })),
      { chunkSize: 1 },
    );
    const results = yield* transfers.uploadMany(requests, 2).pipe(Stream.runCollect);
    assert.isTrue(results.every(Result.isSuccess));
    const diagnostics = yield* transfers.diagnostics;
    assert.strictEqual(diagnostics.entries.length, 128);
    assert.strictEqual(diagnostics.dropped, 72);
  }),
);

it.effect("rejects mismatched or oversized part identities before completion", () =>
  Effect.gen(function* () {
    for (const wrong of [{ partNumber: 2 }, { size: 1 }, { etag: "x".repeat(257) }]) {
      const test = yield* fixture();
      const transfers = yield* makeTransfers(
        {
          ...test.endpoint,
          multipart: {
            ...test.multipart,
            uploadPart: (request) =>
              test.multipart
                .uploadPart(request)
                .pipe(Effect.map((receipt) => ({ ...receipt, ...wrong }))),
          },
        },
        { ...policy, partConcurrency: 1 },
      );
      const result = yield* transfers
        .upload({ key: "identity", source: UploadSource.bytes(new Uint8Array(32)) })
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result))
        assert.strictEqual(
          result.failure.reason._tag,
          "size" in wrong ? "LengthMismatch" : "IntegrityMismatch",
        );
      assert.strictEqual(test.counts().completes, 0);
      assert.strictEqual(test.counts().aborts, 1);
      assert.strictEqual((yield* transfers.resources).reservedBytes, 0);
    }
  }),
);
