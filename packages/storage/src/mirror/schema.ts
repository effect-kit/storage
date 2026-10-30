import { Schema } from "effect";

import { StorageError } from "../errors.js";
import { ObjectMetadata } from "../object.js";

export const MirrorOutcome = Schema.Struct({
  storeId: Schema.String,
  required: Schema.Boolean,
  state: Schema.Literals(["pending", "succeeded", "unknown", "conflict"]),
  metadata: Schema.optional(ObjectMetadata),
  error: Schema.optional(StorageError),
});
export const MirrorOperation = Schema.Struct({
  id: Schema.String,
  binding: Schema.String,
  generation: Schema.Natural,
  key: Schema.String,
  kind: Schema.Literals(["put", "delete"]),
  requestHash: Schema.optional(Schema.String),
  state: Schema.Literals(["pending", "completed", "partial", "superseded"]),
  sha256: Schema.String,
  size: Schema.Natural,
  createdAt: Schema.Number,
  outcomes: Schema.Array(MirrorOutcome),
});
export type MirrorOperation = typeof MirrorOperation.Type;
export const MirrorPointer = Schema.Struct({
  id: Schema.String,
  generation: Schema.Natural,
  kind: Schema.Literals(["put", "delete"]),
});
export const MirrorRepairPlan = Schema.Struct({
  id: Schema.String,
  binding: Schema.String,
  count: Schema.Natural,
  offset: Schema.Natural,
  state: Schema.Literals(["planned", "applying", "completed", "partial"]),
  unresolved: Schema.Natural,
  prefix: Schema.String,
});
export type MirrorRepairPlan = typeof MirrorRepairPlan.Type;
