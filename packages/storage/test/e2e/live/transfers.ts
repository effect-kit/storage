import assert from "node:assert/strict";

import { Deferred, Effect, Fiber, Result } from "effect";

import { Storage, StorageError } from "@effect-kit/storage";
import type { MultipartHandle, StorageService } from "@effect-kit/storage";
import { TransferDriver, Transfers } from "@effect-kit/storage/transfers";
import type { TransferEndpoint } from "@effect-kit/storage/transfers";

import { generatedSource, ownedObjects, partBytes, transferBytes } from "./fixtures.ts";
import type { SuiteReport } from "./suite.ts";

export const transferPolicy = {
  partBytes,
  partConcurrency: 1,
  maxBytes: 16 * 1024 ** 2,
  maxAttempts: 32,
  deadlineMillis: 120000,
  cleanupTimeoutMillis: 15000,
};

export const transferSuite = Effect.fnUntraced(function* (
  storage: StorageService,
  endpoint: TransferEndpoint,
  prefix: string,
  report: SuiteReport,
) {
  const objects = yield* ownedObjects(storage, prefix, report);
  let lostResponse = false;
  const sentParts: number[] = [];
  const protocol = endpoint.multipart;
  const driver: TransferEndpoint = {
    ...endpoint,
    ...(protocol
      ? {
          multipart: {
            ...protocol,
            uploadPart: (request) =>
              protocol.uploadPart(request).pipe(
                Effect.flatMap((receipt) => {
                  sentParts.push(request.partNumber);
                  if (!lostResponse) {
                    lostResponse = true;
                    return Effect.fail(
                      new StorageError({
                        operation: "acceptance.lost-part-response",
                        provider: storage.provider,
                        reason: { _tag: "TransportFailure" },
                        mutationOutcome: "unknown",
                      }),
                    );
                  }
                  return Effect.succeed(receipt);
                }),
              ),
          },
        }
      : {}),
  };
  const transfers = yield* Transfers.make(transferPolicy).pipe(
    Effect.provideService(TransferDriver, driver),
    Effect.provideService(Storage, storage),
  );
  const owned = objects.reserve();
  const receipt = yield* transfers.upload({
    key: `${prefix}large`,
    source: generatedSource(),
    strategy: endpoint.offsets ? "offset" : endpoint.blocks ? "blocks" : "multipart",
  });
  owned.ref = receipt.metadata.ref;
  assert.equal((yield* storage.head({ ref: receipt.metadata.ref })).size, transferBytes);
  assert.notEqual(receipt.strategy, "single");

  let downloaded = 0;
  yield* transfers.download({
    ref: receipt.metadata.ref,
    expectedChecksum: receipt.checksum,
    target: {
      write: (chunk) =>
        Effect.sync(() => {
          assert(chunk.every((byte, index) => byte === (downloaded + index) % 251));
          downloaded += chunk.length;
        }),
    },
  });
  assert.equal(downloaded, transferBytes);
  report.checks.push("large-native-transfer");

  if (endpoint.multipart) {
    assert(lostResponse && sentParts.filter((part) => part === 1).length === 2);
    assert(new Set(sentParts).size >= 3);
    report.checks.push("lost-part-response-retried");
  } else report.checks.push("offset-transfer");

  const badChecksum = yield* Effect.result(
    transfers.download({
      ref: receipt.metadata.ref,
      expectedChecksum: { ...receipt.checksum, value: "0".repeat(64) },
      target: { write: () => Effect.void },
    }),
  );
  assert(Result.isFailure(badChecksum) && badChecksum.failure.reason._tag === "IntegrityMismatch");
  report.checks.push("checksum-mismatch-rejected");

  if (endpoint.multipart && endpoint.handles) {
    const multipart = endpoint.multipart;
    const acknowledged = yield* Deferred.make<void>();
    let upload: MultipartHandle | undefined;
    let aborted = false;
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        if (!upload || aborted) return;
        const cleanup = yield* Effect.result(multipart.abort(upload));
        if (Result.isFailure(cleanup)) report.cleanupPending++;
      }),
    );
    const cancellable = yield* Transfers.make(transferPolicy).pipe(
      Effect.provideService(Storage, storage),
      Effect.provideService(TransferDriver, {
        ...endpoint,
        multipart: {
          ...multipart,
          initiate: (request) =>
            multipart.initiate(request).pipe(
              Effect.tap((handle) =>
                Effect.sync(() => {
                  upload = handle;
                }),
              ),
            ),
          uploadPart: (request) =>
            request.partNumber === 1
              ? multipart
                  .uploadPart(request)
                  .pipe(Effect.tap(() => Deferred.succeed(acknowledged, undefined)))
              : Effect.never,
          abort: (handle) =>
            multipart.abort(handle).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  aborted = true;
                }),
              ),
            ),
        },
      }),
    );
    const fiber = yield* Effect.forkChild(
      cancellable.upload({
        key: `${prefix}cancelled`,
        source: generatedSource(),
        strategy: "multipart",
      }),
    );
    yield* Deferred.await(acknowledged).pipe(Effect.timeout("60 seconds"));
    yield* Fiber.interrupt(fiber);
    assert(upload && aborted);
    const remaining = yield* Effect.result(multipart.listParts({ upload }));
    assert(Result.isFailure(remaining) && remaining.failure.reason._tag === "InvalidSession");
    const diagnostics = yield* cancellable.diagnostics;
    assert.equal(diagnostics.entries.length, 0);
    report.checks.push("cancelled-multipart-cleaned");
  } else report.checks.push("multipart-cancellation-unavailable");

  const resources = yield* transfers.resources;
  assert.equal(resources.activeRequests, 0);
  assert.equal(resources.reservedBytes, 0);
  report.checks.push("transfer-resources-released");
});
