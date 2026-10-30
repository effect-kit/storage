import { Schema } from "effect";

import { ObjectMetadata } from "../object.js";
export const ContentId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/u));
export const ContentRecord = Schema.Struct({
  id: ContentId,
  binding: Schema.String,
  generation: Schema.Natural,
  state: Schema.Literals(["writing", "ready", "deleting", "deleted"]),
  size: Schema.Natural,
  createdAt: Schema.Number,
  retainUntil: Schema.Number,
  edgeCount: Schema.Natural,
  object: Schema.optional(ObjectMetadata),
});
export type ContentRecord = typeof ContentRecord.Type;
export const ReferenceName = Schema.String.check(
  Schema.isMinLength(1),
  Schema.makeFilter<string>((value) => value.isWellFormed()),
  Schema.isMaxLength(256),
  Schema.isPattern(/^[^\0\r\n]+$/u),
);
export const ReferencePointer = Schema.Struct({
  reference: ReferenceName,
  intentId: Schema.String,
  contentId: Schema.optional(ContentId),
});
export const ReferenceIntent = Schema.Struct({
  id: Schema.String,
  reference: ReferenceName,
  contentId: Schema.optional(ContentId),
  edge: Schema.optional(Schema.Natural),
  state: Schema.Literals(["pending", "completed"]),
});
export type ReferenceIntent = typeof ReferenceIntent.Type;
export const ReferenceEdge = Schema.Struct({
  reference: ReferenceName,
  intentId: Schema.String,
  state: Schema.Literals(["prepared", "committed"]),
});
export const CollectionPlan = Schema.Struct({
  id: Schema.String,
  binding: Schema.String,
  cutoff: Schema.Number,
  count: Schema.Natural,
  state: Schema.Literals(["planned", "applying", "completed", "partial"]),
});
export type CollectionPlan = typeof CollectionPlan.Type;
export const CollectionEntry = Schema.Struct({
  contentId: ContentId,
  generation: Schema.Natural,
  state: Schema.Literals(["planned", "collected", "retained", "unknown"]),
});
