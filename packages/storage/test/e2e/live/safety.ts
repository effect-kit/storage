import assert from "node:assert/strict";

import { Effect, Result } from "effect";

import { Storage } from "@effect-kit/storage";
import type { StorageService } from "@effect-kit/storage";

import { ownedObjects, verifyBytes } from "./fixtures.ts";
import type { SuiteReport } from "./suite.ts";

export const safetySuite = Effect.fnUntraced(function* (
  storage: StorageService,
  prefix: string,
  report: SuiteReport,
) {
  const objects = yield* ownedObjects(storage, `${prefix}safety/`, report);
  const first = new Uint8Array([1, 2, 3]);
  const second = new Uint8Array([4, 5, 6]);
  const missing = yield* Effect.result(storage.head({ key: `${prefix}never-created` }));
  assert(Result.isFailure(missing) && missing.failure.reason._tag === "NotFound");
  report.checks.push("missing-object");

  const metadata = yield* objects.put("original", first);
  const foreign = yield* Effect.result(
    storage.head({ ref: { ...metadata.ref, storeId: `${metadata.ref.storeId}:foreign` } }),
  );
  assert(Result.isFailure(foreign) && foreign.failure.reason._tag === "InvalidInput");
  report.checks.push("foreign-reference-denied");

  if (storage.capabilities.putConditions.includes("create-only")) {
    const raced = yield* Effect.all(
      [first, second].map((body) =>
        Effect.result(objects.put("race", body, { condition: { _tag: "CreateOnly" } })),
      ),
      { concurrency: 2 },
    );
    const winners = raced.filter(Result.isSuccess);
    const losers = raced.filter(Result.isFailure);
    assert.equal(winners.length, 1);
    assert.equal(losers.length, 1);
    const loser = losers[0];
    const winner = winners[0];
    assert(loser && winner);
    assert.equal(loser.failure.reason._tag, "PreconditionFailed");
    const winningIndex = raced.findIndex(Result.isSuccess);
    const expected = [first, second][winningIndex];
    assert(expected);
    yield* verifyBytes(storage, winner.success.ref, expected);
    report.checks.push("concurrent-create-only");
  } else {
    const unsupported = yield* Effect.result(
      storage.put({ key: `${prefix}unsupported`, body: first, condition: { _tag: "CreateOnly" } }),
    );
    assert(Result.isFailure(unsupported) && unsupported.failure.reason._tag === "Unsupported");
    report.checks.push("create-only-unsupported");
  }

  if (storage.capabilities.putConditions.includes("if-match")) {
    assert(metadata.etag);
    const updated = yield* objects.put("original", second, {
      condition: { _tag: "IfMatch", etag: metadata.etag },
    });
    const stale = yield* Effect.result(
      objects.put("original", first, { condition: { _tag: "IfMatch", etag: metadata.etag } }),
    );
    assert(Result.isFailure(stale) && stale.failure.reason._tag === "PreconditionFailed");
    yield* verifyBytes(storage, updated.ref, second);

    if (metadata.ref.version && storage.capabilities.versionedReads === "history")
      yield* verifyBytes(storage, metadata.ref, first);
    else if (metadata.ref.version && storage.capabilities.versionedReads === "current-only") {
      const old = yield* Effect.result(storage.head({ ref: metadata.ref }));
      assert(Result.isFailure(old) && old.failure.reason._tag === "NotFound");
    } else if (storage.capabilities.getConditions.includes("if-match")) {
      const pinned = yield* Effect.result(
        Storage.getBytes({
          key: metadata.ref.key,
          condition: { _tag: "IfMatch", etag: metadata.etag },
          maxBytes: first.length,
        }).pipe(Effect.provideService(Storage, storage)),
      );
      assert(Result.isFailure(pinned) && pinned.failure.reason._tag === "PreconditionFailed");
    }
    report.checks.push("stale-write-rejected");
  } else report.checks.push("conditional-overwrite-unsupported");
});
