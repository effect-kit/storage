import { Schema } from "effect";

import { Checksum, ObjectHeaders, ObjectKey, ObjectMetadata, ObjectSize } from "../object.js";
import { WriteCondition } from "../requests.js";

export const SessionId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,100}$/u));
const Event = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["completed", "aborted"]),
  delivered: Schema.Boolean,
});
const Base = {
  version: Schema.Literal(1),
  id: SessionId,
  namespace: Schema.String,
  revision: Schema.Natural,
  configurationId: Schema.String,
  provider: Schema.String,
  storeId: Schema.String,
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
  expiresAt: Schema.Number,
};
export const SavedPart = Schema.Struct({
  sourceChecksum: Checksum,
  partNumber: Schema.Natural,
  etag: Schema.String,
  size: ObjectSize,
  checksums: Schema.optional(Schema.Array(Checksum)),
});
export const UploadCheckpoint = Schema.Struct({
  ...Base,
  kind: Schema.Literal("upload"),
  operationId: Schema.String,
  state: Schema.Literals([
    "created",
    "initiating",
    "uploading",
    "completing",
    "completion-unknown",
    "verifying",
    "completed",
    "aborting",
    "cleanup-pending",
    "aborted",
  ]),
  key: ObjectKey,
  headers: ObjectHeaders,
  condition: Schema.optional(WriteCondition),
  length: ObjectSize,
  checksum: Checksum,
  partBytes: ObjectSize,
  handle: Schema.optional(Schema.Redacted(Schema.String)),
  parts: Schema.Array(SavedPart),
  metadata: Schema.optional(ObjectMetadata),
  attempts: Schema.Natural,
  events: Schema.Array(Event),
});
export type UploadCheckpoint = typeof UploadCheckpoint.Type;
export const DownloadCheckpoint = Schema.Struct({
  ...Base,
  kind: Schema.Literal("download"),
  state: Schema.Literals(["downloading", "verified", "completed", "aborting", "aborted"]),
  metadata: ObjectMetadata,
  length: ObjectSize,
  expectedChecksum: Schema.optional(Checksum),
  targetId: Schema.String,
  targetIdentity: Schema.optional(Schema.String),
  offset: ObjectSize,
  prefixChecksum: Schema.optional(Checksum),
  checksum: Schema.optional(Checksum),
});
export type DownloadCheckpoint = typeof DownloadCheckpoint.Type;
export const BatchCheckpoint = Schema.Struct({
  ...Base,
  kind: Schema.Literal("batch"),
  entries: Schema.Array(
    Schema.Struct({
      sessionId: SessionId,
      state: Schema.Literals(["pending", "completed", "failed", "unknown"]),
    }),
  ),
});
export type BatchCheckpoint = typeof BatchCheckpoint.Type;
export const SessionCheckpoint = Schema.Union([
  UploadCheckpoint,
  DownloadCheckpoint,
  BatchCheckpoint,
]);
export type SessionCheckpoint = typeof SessionCheckpoint.Type;

/** Intentionally excludes provider handles, headers and source material. */
export interface SessionStatus {
  readonly id: string;
  readonly kind: SessionCheckpoint["kind"];
  readonly state: string;
  readonly revision: number;
  readonly expired: boolean;
  readonly completedParts?: number;
  readonly offset?: number;
  readonly metadata?: ObjectMetadata | undefined;
}
