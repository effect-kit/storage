import { Schema } from "effect";

import type { ObjectRef } from "../object.js";
import { ObjectMetadata, ObjectSize } from "../object.js";

export const UploadId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/u));
export const UploadActor = Schema.Struct({
  subject: Schema.NonEmptyString,
  tenantId: Schema.optional(Schema.NonEmptyString),
});
export type UploadActor = typeof UploadActor.Type;
export const UploadFile = Schema.Struct({
  name: Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(255),
    Schema.isPattern(/^[^\r\n\0/\\]+$/u),
  ),
  size: ObjectSize,
  contentType: Schema.String.check(Schema.isPattern(/^[\w!#$&^.+-]+\/[\w!#$&^.+-]+$/u)),
  sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/u)),
});
export const UploadInput = Schema.Struct({
  purpose: Schema.NonEmptyString,
  file: UploadFile,
  context: Schema.Record(Schema.String, Schema.String),
  strategy: Schema.optional(
    Schema.Literals(["auto", "single", "post", "multipart", "resumable", "proxy"]),
  ),
});
export type UploadInput = typeof UploadInput.Type;
export const UploadRequest = Schema.Struct({
  version: Schema.Literal(1),
  idempotencyKey: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(200)),
  input: UploadInput,
});
export const UploadErrorCode = Schema.Literals([
  "BadRequest",
  "Unauthorized",
  "Forbidden",
  "NotFound",
  "Conflict",
  "Expired",
  "Unsupported",
  "VerificationFailed",
  "Unavailable",
]);
export class UploadError extends Schema.TaggedError<UploadError>()("UploadError", {
  code: UploadErrorCode,
}) {}
export const failUpload = (code: typeof UploadErrorCode.Type) => new UploadError({ code });
export const UploadState = Schema.Literals([
  "reserving",
  "ready",
  "initiating",
  "uploading",
  "verifying",
  "publishing",
  "published",
  "aborted",
  "rejected",
]);
export const UploadStatus = Schema.Struct({
  version: Schema.Literal(1),
  id: UploadId,
  state: UploadState,
  expiresAt: Schema.Number,
  cleanupPending: Schema.Boolean,
  result: Schema.optional(Schema.Unknown),
});
export type UploadStatus = typeof UploadStatus.Type;
const Constraints = Schema.optional(
  Schema.Array(
    Schema.Struct({
      kind: Schema.String,
      enforcement: Schema.Literals(["provider", "backend", "advisory"]),
      value: Schema.Union([Schema.String, Schema.Number]),
    }),
  ),
);
const RequestGrant = Schema.Struct({
  _tag: Schema.Literal("SignedRequest"),
  constraints: Constraints,
  method: Schema.Literal("PUT"),
  url: Schema.String,
  headers: Schema.Record(Schema.String, Schema.String),
  expiresAt: Schema.Number,
  partReceipt: Schema.optional(Schema.String),
});
const PostGrant = Schema.Struct({
  _tag: Schema.Literal("PostForm"),
  constraints: Constraints,
  url: Schema.String,
  fields: Schema.Record(Schema.String, Schema.String),
  expiresAt: Schema.Number,
});
const MultipartGrant = Schema.Struct({
  _tag: Schema.Literal("Multipart"),
  constraints: Constraints,
  partBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  expiresAt: Schema.Number,
  completed: Schema.Array(Schema.Struct({ partNumber: Schema.Natural, etag: Schema.String })),
});
const ResumableGrant = Schema.Struct({
  _tag: Schema.Literal("Resumable"),
  constraints: Constraints,
  protocol: Schema.Literals(["gcs", "tus"]),
  url: Schema.String,
  headers: Schema.Record(Schema.String, Schema.String),
  offset: ObjectSize,
  chunkBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  expiresAt: Schema.Number,
});
const BlocksGrant = Schema.Struct({
  _tag: Schema.Literal("Blocks"),
  constraints: Constraints,
  partBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  completed: Schema.Array(Schema.Natural),
  expiresAt: Schema.Number,
});
export const UploadGrant = Schema.Union([
  RequestGrant,
  PostGrant,
  MultipartGrant,
  BlocksGrant,
  ResumableGrant,
  Schema.Struct({
    _tag: Schema.Literal("Proxy"),
    constraints: Constraints,
    expiresAt: Schema.Number,
  }),
]);
export type UploadGrant = typeof UploadGrant.Type;
export const GrantResponse = Schema.Struct({
  version: Schema.Literal(1),
  id: UploadId,
  grant: UploadGrant,
});
export const PartRequest = Schema.Struct({
  partNumber: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10000 })),
});
export const PartsRequest = Schema.Struct({
  parts: Schema.Array(
    Schema.Struct({ partNumber: PartRequest.fields.partNumber, receipt: Schema.NonEmptyString }),
  ).check(Schema.isMaxLength(10000)),
});
export const UploadRecord = Schema.Struct({
  version: Schema.Literal(1),
  id: UploadId,
  actor: UploadActor,
  input: UploadInput,
  storeId: Schema.String,
  key: Schema.String,
  stagingKey: Schema.String,
  publishedKey: Schema.String,
  context: Schema.Record(Schema.String, Schema.String),
  expiresAt: Schema.Number,
  grantExpiresAt: Schema.Number,
  state: UploadState,
  cleanupPending: Schema.Boolean,
  remote: Schema.optional(Schema.Redacted(Schema.Unknown)),
  object: Schema.optional(ObjectMetadata),
  result: Schema.optional(Schema.Unknown),
});
export type UploadRecord = typeof UploadRecord.Type;
export type PublishedObject = typeof ObjectRef.Type;
