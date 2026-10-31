import assert from "node:assert/strict";

import { Effect, Result, Stream } from "effect";

import { Storage } from "@effect-kit/storage";
import type { ObjectRef, PutRequest, StorageService } from "@effect-kit/storage";
import { UploadSource } from "@effect-kit/storage/transfers";

import type { SuiteReport } from "./suite.ts";

export const partBytes = 6 * 1024 ** 2;
export const transferBytes = partBytes * 2 + 127;

export function generatedSource() {
  return UploadSource.replayable(
    () =>
      Stream.fromIterable(
        (function* () {
          for (let offset = 0; offset < transferBytes; offset += 65536)
            yield Uint8Array.from(
              { length: Math.min(65536, transferBytes - offset) },
              (_, index) => (offset + index) % 251,
            );
        })(),
        { chunkSize: 1 },
      ),
    transferBytes,
  );
}

/** Unknown write outcomes remain debt; cleanup only deletes exact observed receipts. */
export const ownedObjects = Effect.fnUntraced(function* (
  storage: StorageService,
  prefix: string,
  report: SuiteReport,
) {
  const writes: Array<{ ref?: ObjectRef }> = [];

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      for (const write of writes.toReversed()) {
        if (!write.ref) {
          report.cleanupPending++;
          continue;
        }

        const removed = yield* Effect.result(storage.delete({ ref: write.ref }));
        const observed = yield* Effect.result(storage.head({ ref: write.ref }));
        if (
          (Result.isFailure(removed) && removed.failure.reason._tag !== "NotFound") ||
          Result.isSuccess(observed) ||
          observed.failure.reason._tag !== "NotFound"
        )
          report.cleanupPending++;
      }
    }),
  );

  const reserve = () => {
    const write: { ref?: ObjectRef } = {};
    writes.push(write);
    return write;
  };

  return {
    reserve,
    put: (name: string, body: PutRequest["body"], extra: Partial<PutRequest> = {}) => {
      const write = reserve();
      return storage.put({ ...extra, key: prefix + name, body }).pipe(
        Effect.tap((metadata) =>
          Effect.sync(() => {
            write.ref = metadata.ref;
          }),
        ),
        Effect.tapError((error) =>
          Effect.sync(() => {
            // A definite rejection created no object and needs no remote cleanup.
            if (["not-started", "rejected"].includes(error.mutationOutcome))
              writes.splice(writes.indexOf(write), 1);
          }),
        ),
      );
    },
  };
});

export const verifyBytes = Effect.fnUntraced(function* (
  storage: StorageService,
  ref: ObjectRef,
  expected: Uint8Array,
) {
  const actual = yield* Storage.getBytes({ ref, maxBytes: expected.length }).pipe(
    Effect.provideService(Storage, storage),
  );
  assert.deepEqual(actual, expected);
});
