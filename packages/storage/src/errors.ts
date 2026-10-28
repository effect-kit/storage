import { Schema } from "effect";

export const StorageReason = Schema.Union([
  Schema.TaggedStruct("InvalidInput", { field: Schema.String }),
  Schema.TaggedStruct("InvalidConfiguration", { field: Schema.String }),
  Schema.TaggedStruct("Unsupported", { capability: Schema.String }),
  Schema.TaggedStruct("NotFound", {}),
  Schema.TaggedStruct("AccessDenied", {}),
  Schema.TaggedStruct("PreconditionFailed", {}),
  Schema.TaggedStruct("NotModified", {}),
  Schema.TaggedStruct("RangeNotSatisfiable", {}),
  Schema.TaggedStruct("Expired", {}),
  Schema.TaggedStruct("Throttled", { retryAfterMillis: Schema.optional(Schema.Natural) }),
  Schema.TaggedStruct("TransportFailure", { code: Schema.optional(Schema.String) }),
  Schema.TaggedStruct("ProviderFailure", { code: Schema.optional(Schema.String) }),
  Schema.TaggedStruct("IntegrityMismatch", {}),
  Schema.TaggedStruct("LengthMismatch", { expected: Schema.Natural, actual: Schema.Natural }),
  Schema.TaggedStruct("LimitExceeded", { limit: Schema.Natural }),
  Schema.TaggedStruct("InvalidSession", {}),
  Schema.TaggedStruct("QuotaExceeded", { limit: Schema.Natural }),
  Schema.TaggedStruct("ScopeClosed", {}),
  Schema.TaggedStruct("BodyConsumed", {}),
  Schema.TaggedStruct("PartialFailure", {}),
]);

export type StorageReason = typeof StorageReason.Type;

export const MutationOutcome = Schema.Literals([
  "not-applicable",
  "not-started",
  "rejected",
  "succeeded",
  "unknown",
  "partial",
]);

export type MutationOutcome = typeof MutationOutcome.Type;

/** Contains safe operational context only; raw SDK responses and bearer URLs stay private. */
export class StorageError extends Schema.TaggedError<StorageError>()("StorageError", {
  operation: Schema.String,
  provider: Schema.String,
  reason: StorageReason,
  mutationOutcome: MutationOutcome,
  requestId: Schema.optional(Schema.String),
}) {}

export function storageError(
  operation: string,
  provider: string,
  reason: StorageReason,
  mutationOutcome: MutationOutcome = "not-applicable",
): StorageError {
  return new StorageError({ operation, provider, reason, mutationOutcome });
}
