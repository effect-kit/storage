import { DateTime, Effect, Schema } from "effect";

import type { BlobHTTPHeaders } from "@azure/storage-blob";

import type { StorageError } from "../../core/errors.js";
import { storageError } from "../../core/errors.js";
import { HeaderValue, ObjectRef } from "../../core/object.js";
import type { ObjectHeaders, ObjectMetadata } from "../../core/object.js";
import type { ObjectTarget, WriteCondition } from "../../core/requests.js";
import type { AzureConnection } from "./config.js";
import { validateKey } from "./validation.js";

export interface NativeMetadata {
  readonly etag?: string | undefined;
  readonly versionId?: string | undefined;
  readonly lastModified?: Date | undefined;
  readonly contentLength?: number | undefined;
  readonly contentType?: string | undefined;
  readonly contentEncoding?: string | undefined;
  readonly contentLanguage?: string | undefined;
  readonly contentDisposition?: string | undefined;
  readonly cacheControl?: string | undefined;
  readonly metadata?: Record<string, string> | undefined;
  readonly contentMD5?: Uint8Array | undefined;
  readonly blobType?: string | undefined;
}
export function metadata(
  connection: AzureConnection,
  key: string,
  value: NativeMetadata,
  ref?: ObjectRef,
): ObjectMetadata {
  return {
    ref: {
      storeId: connection.config.storeId,
      key,
      ...(value.versionId
        ? { version: { _tag: "AzureVersion", id: value.versionId } }
        : ref?.version
          ? { version: ref.version }
          : {}),
    },
    etag: value.etag,
    size: value.contentLength,
    lastModified: value.lastModified ? DateTime.makeUnsafe(value.lastModified) : undefined,
    contentType: value.contentType,
    contentEncoding: value.contentEncoding,
    contentLanguage: value.contentLanguage,
    contentDisposition: value.contentDisposition,
    cacheControl: value.cacheControl,
    customMetadata: value.metadata
      ? Object.fromEntries(
          Object.entries(value.metadata).map(([name, content]) => [
            name === "effect_storage_operation" ? "effect-storage-operation" : name,
            content,
          ]),
        )
      : undefined,
    checksums: value.contentMD5
      ? [
          {
            algorithm: "md5",
            encoding: "base64",
            scope: "full-object",
            value: Buffer.from(value.contentMD5).toString("base64"),
          },
        ]
      : undefined,
  };
}
export const target = Effect.fnUntraced(function* (
  connection: AzureConnection,
  request: ObjectTarget,
  operation: string,
) {
  const ref =
    "ref" in request ? request.ref : { key: request.key, storeId: connection.config.storeId };
  yield* Schema.decodeUnknownEffect(ObjectRef)(ref).pipe(
    Effect.mapError(() => storageError(operation, "azure", { _tag: "InvalidInput", field: "ref" })),
  );
  if (
    ref.storeId !== connection.config.storeId ||
    (ref.version && !["AzureVersion", "AzureSnapshot"].includes(ref.version._tag))
  )
    return yield* storageError(operation, "azure", { _tag: "InvalidInput", field: "ref" });
  yield* validateKey(ref.key, operation, ["delete", "blocks"].includes(operation));
  let client = connection.client.getBlobClient(ref.key);
  if (ref.version)
    client =
      ref.version._tag === "AzureSnapshot"
        ? client.withSnapshot(ref.version.id)
        : client.withVersion(ref.version.id);
  return { ref, client };
});
export function nativeHeaders(headers: ObjectHeaders): BlobHTTPHeaders {
  return {
    ...(headers.contentType !== undefined ? { blobContentType: headers.contentType } : {}),
    ...(headers.contentEncoding !== undefined
      ? { blobContentEncoding: headers.contentEncoding }
      : {}),
    ...(headers.contentLanguage !== undefined
      ? { blobContentLanguage: headers.contentLanguage }
      : {}),
    ...(headers.contentDisposition !== undefined
      ? { blobContentDisposition: headers.contentDisposition }
      : {}),
    ...(headers.cacheControl !== undefined ? { blobCacheControl: headers.cacheControl } : {}),
  };
}
export function condition(value?: WriteCondition) {
  return value?._tag === "CreateOnly" ? { ifNoneMatch: "*" } : value ? { ifMatch: value.etag } : {};
}

const AsciiHeader = HeaderValue.check(Schema.isPattern(/^[\x20-\x7e]*$/u));
const MetadataKey = Schema.String.check(
  Schema.isPattern(
    /^(?!effect_storage_operation$)(?:effect-storage-operation|[A-Za-z_][A-Za-z0-9_]*)$/u,
  ),
);
const AzureHeaders = Schema.Struct({
  contentType: Schema.optional(AsciiHeader),
  contentEncoding: Schema.optional(AsciiHeader),
  contentLanguage: Schema.optional(AsciiHeader),
  contentDisposition: Schema.optional(AsciiHeader),
  cacheControl: Schema.optional(AsciiHeader),
  customMetadata: Schema.optional(Schema.Record(MetadataKey, AsciiHeader)),
});
export const writeMetadata = (
  headers: ObjectHeaders,
): Effect.Effect<Record<string, string>, StorageError> =>
  Schema.decodeUnknownEffect(AzureHeaders)(headers).pipe(
    Effect.mapError(() =>
      storageError("headers", "azure", { _tag: "InvalidInput", field: "headers" }, "not-started"),
    ),
    Effect.map((value) =>
      Object.fromEntries(
        Object.entries(value.customMetadata ?? {}).map(([key, content]) => [
          key === "effect-storage-operation" ? "effect_storage_operation" : key,
          content,
        ]),
      ),
    ),
  );
