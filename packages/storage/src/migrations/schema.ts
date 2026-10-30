import { Schema } from "effect";

import { StorageError } from "../errors.js";
import { ObjectMetadata } from "../object.js";

export const MigrationId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,128}$/u));
export const MigrationRequest = Schema.Struct({
  id: Schema.optional(MigrationId),
  inventoryId: Schema.optional(Schema.NonEmptyString),
  prefix: Schema.String,
  destinationPrefix: Schema.optional(Schema.String),
  conflict: Schema.Literals(["fail", "skip", "replace-if-unchanged"]),
  deleteSource: Schema.Boolean,
  deleteExtraneous: Schema.optional(Schema.Boolean),
});
export type MigrationRequest = typeof MigrationRequest.Type;
export const MigrationPlan = Schema.Struct({
  id: MigrationId,
  binding: Schema.String,
  request: MigrationRequest,
  state: Schema.Literals(["planning", "planned", "applying", "completed", "partial"]),
  count: Schema.Natural,
  createdAt: Schema.Number,
});
export type MigrationPlan = typeof MigrationPlan.Type;
export const MigrationEntry = Schema.Struct({
  index: Schema.Natural,
  action: Schema.Literals(["copy", "delete"]),
  source: ObjectMetadata,
  destinationKey: Schema.String,
  previous: Schema.optional(ObjectMetadata),
  sha256: Schema.String,
  state: Schema.Literals([
    "planned",
    "skipped",
    "copying",
    "copied",
    "deleting",
    "completed",
    "conflict",
    "unknown",
    "failed",
  ]),
  destination: Schema.optional(ObjectMetadata),
  strategy: Schema.optional(Schema.String),
  error: Schema.optional(StorageError),
});
export type MigrationEntry = typeof MigrationEntry.Type;
