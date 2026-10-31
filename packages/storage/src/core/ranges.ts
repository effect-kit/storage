import { Result, Schema } from "effect";

import { storageError } from "./errors.js";
import type { StorageError } from "./errors.js";
import { ObjectSize } from "./object.js";

export const ByteRange = Schema.Union([
  Schema.TaggedStruct("Bounded", { start: ObjectSize, end: ObjectSize }),
  Schema.TaggedStruct("From", { start: ObjectSize }),
  Schema.TaggedStruct("Suffix", { length: Schema.Int.check(Schema.isGreaterThan(0)) }),
]);

export type ByteRange = typeof ByteRange.Type;

export interface ResolvedRange {
  readonly start: number;
  readonly end: number;
  readonly totalSize: number;
}

/** Resolves inclusive HTTP-style bounds against a known complete object size. */
export function resolveRange(
  range: ByteRange,
  size: number,
  provider: string,
): Result.Result<ResolvedRange, StorageError> {
  const start = range._tag === "Suffix" ? Math.max(0, size - range.length) : range.start;
  const end = range._tag === "Bounded" ? Math.min(size - 1, range.end) : size - 1;

  if (size === 0 || start >= size || end < start) {
    return Result.fail(storageError("get", provider, { _tag: "RangeNotSatisfiable" }));
  }

  return Result.succeed({ start, end, totalSize: size });
}
