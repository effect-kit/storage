import { Effect, Redacted, Schema } from "effect";

import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  ListMultipartUploadsCommand,
  ListPartsCommand,
  UploadPartCommand,
} from "@aws-sdk/client-s3";

import type { MultipartHandle, MultipartUploads, UploadedPart } from "../contracts/multipart.js";
import type { StorageError } from "../errors.js";
import { storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { ObjectHeaders } from "../object.js";
import type { UploadBody } from "../requests.js";
import { ListRequest, PutRequest, WriteCondition } from "../requests.js";
import { authority, encryptionHeaders } from "./config.js";
import type { S3Connection } from "./config.js";
import { decodeCursor, encodeCursor } from "./listing.js";
import {
  ChecksumOptions,
  CompletePart,
  PartNumber,
  Upload,
  nativeLimits,
} from "./native-models.js";
import { checksums, condition, headers, metadata, validateRef } from "./object-mapping.js";
import { request } from "./transport.js";
import { uploadBody } from "./upload-body.js";
import { S3Key, validateHeaders } from "./validation.js";

export type S3MultipartHandle = typeof Upload.Type;
export type S3ChecksumOptions = typeof ChecksumOptions.Type;

export interface S3MultipartUploads extends Omit<
  MultipartUploads,
  "initiate" | "uploadPart" | "complete"
> {
  readonly initiate: (input: {
    readonly key: string;
    readonly headers?: ObjectHeaders;
    readonly checksum?: S3ChecksumOptions;
  }) => Effect.Effect<S3MultipartHandle, StorageError>;
  readonly uploadPart: <E = never, R = never>(input: {
    readonly upload: MultipartHandle;
    readonly partNumber: number;
    readonly contentLength?: number;
    readonly body: UploadBody<E, R>;
    readonly checksum?: S3ChecksumOptions;
  }) => Effect.Effect<UploadedPart, StorageError | E, R>;
  readonly complete: (
    input: Parameters<MultipartUploads["complete"]>[0] & { readonly checksum?: S3ChecksumOptions },
  ) => ReturnType<MultipartUploads["complete"]>;
  readonly listUploads: (
    input: ListRequest,
  ) => Effect.Effect<
    { readonly uploads: ReadonlyArray<S3MultipartHandle>; readonly cursor?: string },
    StorageError
  >;
  readonly listParts: (input: {
    readonly upload: MultipartHandle;
    readonly cursor?: string;
  }) => Effect.Effect<
    {
      readonly parts: ReadonlyArray<UploadedPart>;
      readonly cursor?: string;
      readonly checksumAlgorithm?: S3ChecksumOptions["ChecksumAlgorithm"];
      readonly checksumType?: S3ChecksumOptions["ChecksumType"];
    },
    StorageError
  >;
}

const Initiate = Schema.Struct({
  key: S3Key,
  headers: Schema.optional(ObjectHeaders),
  checksum: Schema.optional(ChecksumOptions),
});
const Part = Schema.Struct({
  upload: Upload,
  partNumber: PartNumber,
  body: Schema.Unknown,
  contentLength: PutRequest.fields.contentLength,
  checksum: Schema.optional(ChecksumOptions),
});
const Complete = Schema.Struct({
  upload: Upload,
  parts: Schema.Array(CompletePart).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(nativeLimits.maximumParts),
  ),
  condition: Schema.optional(WriteCondition),
  checksum: Schema.optional(ChecksumOptions),
});
const PartsPage = Schema.Struct({ upload: Upload, cursor: Schema.optional(Schema.NonEmptyString) });

const validateUpload = Effect.fnUntraced(function* (
  connection: S3Connection,
  upload: MultipartHandle,
  operation: string,
) {
  yield* validateRef(upload.ref, connection.config, operation);
  if (upload.ref.version)
    return yield* storageError(
      operation,
      "s3",
      { _tag: "InvalidInput", field: "upload.ref.version" },
      "not-started",
    );
});

function partChecksums(part: UploadedPart) {
  const fields: Record<string, string> = {};
  for (const checksum of part.checksums ?? []) {
    const names: Readonly<Record<string, string>> = {
      crc32: "ChecksumCRC32",
      crc32c: "ChecksumCRC32C",
      crc64nvme: "ChecksumCRC64NVME",
      sha1: "ChecksumSHA1",
      sha256: "ChecksumSHA256",
      sha512: "ChecksumSHA512",
      md5: "ChecksumMD5",
      xxhash64: "ChecksumXXHASH64",
      xxhash3: "ChecksumXXHASH3",
      xxhash128: "ChecksumXXHASH128",
    };
    const name = Object.hasOwn(names, checksum.algorithm) ? names[checksum.algorithm] : undefined;
    if (!name || checksum.encoding !== "base64")
      return Effect.fail(
        storageError(
          "complete",
          "s3",
          { _tag: "Unsupported", capability: "part-checksum-encoding-or-algorithm" },
          "not-started",
        ),
      );
    fields[name] = checksum.value;
  }
  return Effect.succeed(Object.assign(fields, { PartNumber: part.partNumber, ETag: part.etag }));
}

export function multipart(connection: S3Connection): S3MultipartUploads {
  const { client, config } = connection;
  return {
    limits: nativeLimits,
    initiate: Effect.fnUntraced(function* (input) {
      const value = yield* decodeRequest(Initiate, input, "initiate", "s3", true);
      if (value.headers) yield* validateHeaders(value.headers, "initiate");
      const checksumAlgorithm = value.checksum?.ChecksumAlgorithm ?? "CRC32";
      const checksumType =
        value.checksum?.ChecksumType ??
        (checksumAlgorithm === "CRC64NVME" ? "FULL_OBJECT" : "COMPOSITE");
      const output = yield* request(connection, "initiate", true, (signal) =>
        client.send(
          new CreateMultipartUploadCommand({
            ...authority(config),
            ...encryptionHeaders(config.encryption),
            ...headers(value.headers ?? {}),
            ...value.checksum,
            ChecksumAlgorithm: checksumAlgorithm,
            ChecksumType: checksumType,
            Key: value.key,
          }),
          { abortSignal: signal },
        ),
      );
      if (!output.UploadId)
        return yield* storageError(
          "initiate",
          "s3",
          { _tag: "ProviderFailure", code: "MissingUploadId" },
          "unknown",
        );
      return {
        ref: { storeId: config.storeId, key: value.key },
        uploadId: Redacted.make(output.UploadId),
        checksumAlgorithm: output.ChecksumAlgorithm ?? checksumAlgorithm,
        checksumType: output.ChecksumType ?? checksumType,
      };
    }),
    uploadPart: Effect.fnUntraced(function* <E = never, R = never>(input: {
      readonly upload: MultipartHandle;
      readonly partNumber: number;
      readonly body: UploadBody<E, R>;
      readonly contentLength?: number;
      readonly checksum?: S3ChecksumOptions;
    }) {
      const value = yield* decodeRequest(Part, input, "uploadPart", "s3", true);
      yield* validateUpload(connection, value.upload, "uploadPart");
      if (
        value.checksum?.ChecksumType ||
        (value.upload.checksumAlgorithm &&
          value.checksum?.ChecksumAlgorithm &&
          value.upload.checksumAlgorithm !== value.checksum.ChecksumAlgorithm)
      )
        return yield* storageError(
          "uploadPart",
          "s3",
          { _tag: "InvalidInput", field: "checksum" },
          "not-started",
        );
      const output = yield* uploadBody(
        connection,
        {
          body: input.body,
          contentLength: value.contentLength,
          maxBytes: nativeLimits.maximumPartBytes,
        },
        "uploadPart",
        (body, length) =>
          request(connection, "uploadPart", true, (signal) =>
            client.send(
              new UploadPartCommand({
                ...authority(config),
                ...encryptionHeaders(config.encryption),
                ChecksumAlgorithm: value.upload.checksumAlgorithm,
                ...value.checksum,
                Key: value.upload.ref.key,
                UploadId: Redacted.value(value.upload.uploadId),
                PartNumber: value.partNumber,
                Body: body,
                ContentLength: length,
              }),
              { abortSignal: signal },
            ),
          ).pipe(Effect.map((result) => ({ result, length }))),
      );
      if (!output.result.ETag)
        return yield* storageError(
          "uploadPart",
          "s3",
          { _tag: "ProviderFailure", code: "MissingPartETag" },
          "unknown",
        );
      return {
        partNumber: value.partNumber,
        etag: output.result.ETag,
        size: output.length,
        checksums: checksums(output.result),
      };
    }),
    listParts: Effect.fnUntraced(function* (input) {
      const value = yield* decodeRequest(PartsPage, input, "listParts", "s3");
      yield* validateUpload(connection, value.upload, "listParts");
      const query = JSON.stringify([
        "parts",
        value.upload.ref.key,
        Redacted.value(value.upload.uploadId),
      ]);
      const marker = yield* decodeCursor(value.cursor, config.storeId, query, "listParts");
      const output = yield* request(connection, "listParts", false, (signal) =>
        client.send(
          new ListPartsCommand({
            ...authority(config),
            ...encryptionHeaders(config.encryption),
            Key: value.upload.ref.key,
            UploadId: Redacted.value(value.upload.uploadId),
            PartNumberMarker: marker,
            MaxParts: nativeLimits.maximumPageSize,
          }),
          { abortSignal: signal },
        ),
      );
      const parts: UploadedPart[] = [];
      for (const part of output.Parts ?? []) {
        if (!part.ETag || part.PartNumber === undefined)
          return yield* storageError("listParts", "s3", {
            _tag: "ProviderFailure",
            code: "InvalidPartInventory",
          });
        parts.push({
          partNumber: part.PartNumber,
          etag: part.ETag,
          size: part.Size,
          checksums: checksums(part),
        });
      }
      if (output.IsTruncated && !output.NextPartNumberMarker)
        return yield* storageError("listParts", "s3", {
          _tag: "ProviderFailure",
          code: "MissingContinuation",
        });
      return {
        parts,
        checksumAlgorithm: output.ChecksumAlgorithm,
        checksumType: output.ChecksumType,
        ...(output.IsTruncated && output.NextPartNumberMarker
          ? { cursor: encodeCursor(config.storeId, query, output.NextPartNumberMarker) }
          : {}),
      };
    }),
    complete: Effect.fnUntraced(function* (input) {
      const value = yield* decodeRequest(Complete, input, "complete", "s3", true);
      yield* validateUpload(connection, value.upload, "complete");
      if (
        value.checksum?.ChecksumAlgorithm ||
        (value.upload.checksumType &&
          value.checksum?.ChecksumType &&
          value.upload.checksumType !== value.checksum.ChecksumType)
      )
        return yield* storageError(
          "complete",
          "s3",
          { _tag: "InvalidInput", field: "checksum" },
          "not-started",
        );
      const parts = value.parts.toSorted((a, b) => a.partNumber - b.partNumber);
      if (new Set(parts.map((part) => part.partNumber)).size !== parts.length)
        return yield* storageError(
          "complete",
          "s3",
          { _tag: "InvalidInput", field: "duplicate-part" },
          "not-started",
        );
      if (
        parts
          .slice(0, -1)
          .some((part) => part.size !== undefined && part.size < nativeLimits.minimumPartBytes)
      )
        return yield* storageError(
          "complete",
          "s3",
          { _tag: "InvalidInput", field: "part.size" },
          "not-started",
        );
      const nativeParts = yield* Effect.forEach(parts, partChecksums);
      const output = yield* request(connection, "complete", true, (signal) =>
        client.send(
          new CompleteMultipartUploadCommand({
            ...authority(config),
            ...encryptionHeaders(config.encryption),
            ...condition(value.condition),
            ChecksumType: value.upload.checksumType,
            ...value.checksum,
            Key: value.upload.ref.key,
            UploadId: Redacted.value(value.upload.uploadId),
            MultipartUpload: { Parts: nativeParts },
          }),
          { abortSignal: signal },
        ),
      );
      if (!output.ETag)
        return yield* storageError(
          "complete",
          "s3",
          { _tag: "ProviderFailure", code: "MissingCompleteReceipt" },
          "unknown",
        );
      return metadata(config, value.upload.ref.key, output);
    }),
    abort: Effect.fnUntraced(function* (input) {
      const value = yield* decodeRequest(Upload, input, "abort", "s3", true);
      yield* validateUpload(connection, value, "abort");
      yield* request(connection, "abort", true, (signal) =>
        client.send(
          new AbortMultipartUploadCommand({
            ...authority(config),
            Key: value.ref.key,
            UploadId: Redacted.value(value.uploadId),
          }),
          { abortSignal: signal },
        ),
      );
    }),
    listUploads: Effect.fnUntraced(function* (input) {
      const value = yield* decodeRequest(ListRequest, input, "listUploads", "s3");
      if (value.delimiter)
        return yield* storageError("listUploads", "s3", {
          _tag: "Unsupported",
          capability: "upload-common-prefixes",
        });
      if ((value.limit ?? 1000) > nativeLimits.maximumPageSize)
        return yield* storageError("listUploads", "s3", { _tag: "InvalidInput", field: "limit" });
      const query = JSON.stringify(["uploads", value.prefix ?? "", value.limit ?? 1000]);
      const token = yield* decodeCursor(value.cursor, config.storeId, query, "listUploads");
      const marker =
        token === undefined
          ? undefined
          : yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(Schema.Struct({ key: Schema.String, upload: Schema.String })),
            )(token).pipe(
              Effect.mapError(() =>
                storageError("listUploads", "s3", { _tag: "InvalidInput", field: "cursor" }),
              ),
            );
      const output = yield* request(connection, "listUploads", false, (signal) =>
        client.send(
          new ListMultipartUploadsCommand({
            ...authority(config),
            Prefix: value.prefix,
            MaxUploads: value.limit ?? 1000,
            KeyMarker: marker?.key,
            UploadIdMarker: marker?.upload,
          }),
          { abortSignal: signal },
        ),
      );
      const uploads: S3MultipartHandle[] = [];
      for (const upload of output.Uploads ?? []) {
        if (!upload.Key || !upload.UploadId)
          return yield* storageError("listUploads", "s3", {
            _tag: "ProviderFailure",
            code: "InvalidUploadInventory",
          });
        uploads.push({
          ref: { storeId: config.storeId, key: upload.Key },
          uploadId: Redacted.make(upload.UploadId),
          checksumAlgorithm: upload.ChecksumAlgorithm,
          checksumType: upload.ChecksumType,
        });
      }
      if (output.IsTruncated && (!output.NextKeyMarker || !output.NextUploadIdMarker))
        return yield* storageError("listUploads", "s3", {
          _tag: "ProviderFailure",
          code: "MissingContinuation",
        });
      return {
        uploads,
        ...(output.IsTruncated
          ? {
              cursor: encodeCursor(
                config.storeId,
                query,
                JSON.stringify({ key: output.NextKeyMarker, upload: output.NextUploadIdMarker }),
              ),
            }
          : {}),
      };
    }),
  };
}
