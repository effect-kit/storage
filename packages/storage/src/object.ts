import { Schema } from "effect";

/** An opaque key. Provider-specific encoding and limits are checked by adapters. */
export const ObjectKey = Schema.NonEmptyString;

export type ObjectKey = typeof ObjectKey.Type;

/** A JSON-safe byte count. Unsafe integers are rejected rather than rounded. */
export const ObjectSize = Schema.Natural;

export type ObjectSize = typeof ObjectSize.Type;
