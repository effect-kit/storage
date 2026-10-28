import { Effect, Result, Schema } from "effect";

import type { StorageError, StorageReason } from "../../errors.js";
import { Storage } from "../../storage.js";
import type { StorageService } from "../../storage.js";

export class ConformanceFailure extends Schema.TaggedError<ConformanceFailure>()(
  "ConformanceFailure",
  {
    testName: Schema.String,
    detail: Schema.String,
  },
) {}

export const check = (condition: boolean, detail: string) =>
  condition ? Effect.void : Effect.fail(new ConformanceFailure({ testName: "assertion", detail }));

export const read = (storage: StorageService, key: string) =>
  Storage.getBytes({ key, maxBytes: 1024 }).pipe(Effect.provideService(Storage, storage));

export const expectReason = <A, R>(
  effect: Effect.Effect<A, StorageError, R>,
  reason: StorageReason["_tag"],
) =>
  Effect.flatMap(Effect.result(effect), (result) =>
    check(
      Result.isFailure(result) && result.failure.reason._tag === reason,
      `Expected storage reason ${reason}`,
    ),
  );

export const bytesEqual = (actual: Uint8Array, expected: Uint8Array) =>
  actual.length === expected.length && actual.every((byte, index) => byte === expected[index]);
