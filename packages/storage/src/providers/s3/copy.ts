import { Effect, Redacted, Schema } from "effect";

import { CopyObjectCommand, UploadPartCopyCommand } from "@aws-sdk/client-s3";

import type { MultipartHandle } from "../../core/contracts/multipart.js";
import { storageError } from "../../core/errors.js";
import type { ObjectRef } from "../../core/object.js";
import type { ByteRange } from "../../core/ranges.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { authority, encryptionHeaders } from "./config.js";
import type { S3Connection } from "./config.js";
import { CopyRequest, PartNumber, Upload } from "./native-models.js";
import type { S3CopyRequest } from "./native-models.js";
import {
  checksums,
  condition,
  encodeCopySource,
  headers,
  metadata,
  validateRef,
} from "./object-mapping.js";
import { enforceProfile } from "./profile.js";
import { request } from "./transport.js";
import { validateHeaders } from "./validation.js";

function sourceEncryption(connection: S3Connection) {
  const encryption = connection.config.encryption;
  return encryption?.mode === "customer"
    ? {
        CopySourceSSECustomerAlgorithm: "AES256",
        CopySourceSSECustomerKey: Redacted.value(encryption.key),
        CopySourceSSECustomerKeyMD5: encryption.keyMD5,
      }
    : {};
}

export const copyObject = Effect.fnUntraced(function* (
  connection: S3Connection,
  input: S3CopyRequest,
  source: S3Connection = connection,
) {
  if (connection.config.endpoint !== source.config.endpoint)
    return yield* storageError(
      "copy",
      "s3",
      { _tag: "Unsupported", capability: "cross-endpoint-copy" },
      "not-started",
    );
  const value = yield* decodeRequest(CopyRequest, input, "copy", "s3", true);
  yield* enforceProfile(connection, "copy", value);
  const profile = connection.profile;
  if (
    profile &&
    (profile.copy === "none" ||
      (profile.copy === "bucket" &&
        (connection.config.bucket !== source.config.bucket ||
          connection.config.encryption?.mode === "customer" ||
          source.config.encryption?.mode === "customer")))
  )
    return yield* storageError(
      "copy",
      profile.provider,
      { _tag: "Unsupported", capability: "native-copy-profile" },
      "not-started",
    );
  yield* validateRef(value.source, source.config, "copy");
  if (value.headers) yield* validateHeaders(value.headers, "copy");
  if (value.headers && value.metadataDirective !== "REPLACE")
    return yield* storageError(
      "copy",
      "s3",
      { _tag: "InvalidInput", field: "metadataDirective" },
      "not-started",
    );
  if (value.tagging && value.taggingDirective !== "REPLACE")
    return yield* storageError(
      "copy",
      "s3",
      { _tag: "InvalidInput", field: "taggingDirective" },
      "not-started",
    );

  const output = yield* request(connection, "copy", true, (signal) =>
    connection.client.send(
      new CopyObjectCommand({
        ...authority(connection.config),
        ...encryptionHeaders(connection.config.encryption),
        ...sourceEncryption(source),
        ...condition(value.condition),
        ...headers(value.headers ?? {}),
        Key: value.destinationKey,
        ExpectedSourceBucketOwner: source.config.expectedBucketOwner,
        CopySource: encodeCopySource(source.config.bucket, value.source),
        CopySourceIfMatch: value.sourceIfMatch,
        MetadataDirective: value.metadataDirective,
        TaggingDirective: value.taggingDirective,
        Tagging: value.tagging,
      }),
      { abortSignal: signal },
    ),
  );
  if (!output.CopyObjectResult?.ETag)
    return yield* storageError(
      "copy",
      "s3",
      { _tag: "ProviderFailure", code: "MissingCopyReceipt" },
      "unknown",
    );
  return metadata(connection, value.destinationKey, {
    ...output,
    ...output.CopyObjectResult,
  });
});

export interface S3CopyPartRequest {
  readonly upload: MultipartHandle;
  readonly partNumber: number;
  readonly source: ObjectRef;
  readonly sourceIfMatch?: string | undefined;
  readonly range?: Extract<ByteRange, { readonly _tag: "Bounded" }>;
}

const CopyPartRequest = Schema.Struct({
  upload: Upload,
  partNumber: PartNumber,
  source: CopyRequest.fields.source,
  sourceIfMatch: CopyRequest.fields.sourceIfMatch,
  range: Schema.optional(
    Schema.TaggedStruct("Bounded", { start: Schema.Natural, end: Schema.Natural }),
  ),
});

export const copyPart = Effect.fnUntraced(function* (
  connection: S3Connection,
  input: S3CopyPartRequest,
  source: S3Connection = connection,
) {
  if (connection.config.endpoint !== source.config.endpoint)
    return yield* storageError(
      "copyPart",
      "s3",
      { _tag: "Unsupported", capability: "cross-endpoint-copy" },
      "not-started",
    );
  const value = yield* decodeRequest(CopyPartRequest, input, "copyPart", "s3", true);
  yield* enforceProfile(connection, "copyPart", value);
  const profile = connection.profile;
  if (
    profile &&
    (profile.copy === "none" ||
      (profile.copy === "bucket" &&
        (connection.config.bucket !== source.config.bucket ||
          connection.config.encryption?.mode === "customer" ||
          source.config.encryption?.mode === "customer")) ||
      (value.sourceIfMatch && !profile.copyPartConditions))
  )
    return yield* storageError(
      "copyPart",
      profile.provider,
      { _tag: "Unsupported", capability: "native-copy-profile" },
      "not-started",
    );
  yield* validateRef(value.upload.ref, connection.config, "copyPart");
  if (value.upload.ref.version)
    return yield* storageError(
      "copyPart",
      "s3",
      { _tag: "InvalidInput", field: "upload.ref.version" },
      "not-started",
    );
  yield* validateRef(value.source, source.config, "copyPart");
  if (value.range && value.range.end < value.range.start)
    return yield* storageError(
      "copyPart",
      "s3",
      { _tag: "InvalidInput", field: "range" },
      "not-started",
    );

  const output = yield* request(connection, "copyPart", true, (signal) =>
    connection.client.send(
      new UploadPartCopyCommand({
        ...authority(connection.config),
        ...sourceEncryption(source),
        ...encryptionHeaders(connection.config.encryption),
        Key: value.upload.ref.key,
        UploadId: Redacted.value(value.upload.uploadId),
        PartNumber: value.partNumber,
        ExpectedSourceBucketOwner: source.config.expectedBucketOwner,
        CopySource: encodeCopySource(source.config.bucket, value.source),
        CopySourceIfMatch: value.sourceIfMatch,
        CopySourceRange: value.range
          ? `bytes=${String(value.range.start)}-${String(value.range.end)}`
          : undefined,
      }),
      { abortSignal: signal },
    ),
  );
  if (!output.CopyPartResult?.ETag)
    return yield* storageError(
      "copyPart",
      "s3",
      { _tag: "ProviderFailure", code: "MissingPartReceipt" },
      "unknown",
    );
  return {
    partNumber: value.partNumber,
    etag: output.CopyPartResult.ETag,
    checksums: checksums(output.CopyPartResult),
  };
});
