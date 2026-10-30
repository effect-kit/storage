import { DateTime, Effect, Option, Schema } from "effect";

import * as Blob from "@vercel/blob";

import { storageError } from "../errors.js";
import type { ObjectHeaders } from "../object.js";
import { ObjectKey, ObjectMetadata } from "../object.js";
import type { VercelConnection } from "./config.js";

export const validateKey = Effect.fnUntraced(function* (key: string, operation: string) {
  yield* Schema.decodeUnknownEffect(ObjectKey)(key).pipe(
    Effect.mapError(() =>
      storageError(operation, "vercel", { _tag: "InvalidInput", field: "key" }, "not-started"),
    ),
  );
  if (
    key.startsWith("/") ||
    /^https?:/iu.test(key) ||
    key.split("/").some((part) => !part || part === "." || part === "..") ||
    new TextEncoder().encode(key).length > 950
  )
    return yield* storageError(
      operation,
      "vercel",
      { _tag: "InvalidInput", field: "key" },
      "not-started",
    );
  return key;
});
export const writeOptions = Effect.fnUntraced(function* (
  headers: ObjectHeaders,
  operation: string,
) {
  const metadata = Object.keys(headers.customMetadata ?? {}).filter(
    (key) => key !== "effect-storage-operation",
  );
  if (
    metadata.length ||
    headers.contentDisposition ||
    headers.contentLanguage ||
    headers.contentEncoding ||
    headers.cacheControl
  )
    return yield* storageError(
      operation,
      "vercel",
      { _tag: "Unsupported", capability: "object-headers" },
      "not-started",
    );
  return {
    ...(headers.contentType ? { contentType: headers.contentType } : {}),
    addRandomSuffix: false,
  };
});
const Observation = Schema.Struct({
  pathname: Schema.String,
  etag: Schema.optional(Schema.String),
  size: Schema.optional(Schema.NullOr(Schema.Natural)),
  contentType: Schema.optional(Schema.NullOr(Schema.String)),
  contentDisposition: Schema.optional(Schema.String),
  cacheControl: Schema.optional(Schema.String),
});
export const metadata = Effect.fnUntraced(function* (
  connection: VercelConnection,
  value: unknown,
  operation: string,
  key?: string,
  mutation = false,
) {
  const observed = yield* Schema.decodeUnknownEffect(Observation)(value).pipe(
    Effect.mapError(() =>
      storageError(
        operation,
        "vercel",
        { _tag: "ProviderFailure" },
        mutation ? "unknown" : "not-applicable",
      ),
    ),
  );
  if (key !== undefined && observed.pathname !== key)
    return yield* storageError(
      operation,
      "vercel",
      { _tag: "IntegrityMismatch" },
      mutation ? "unknown" : "not-applicable",
    );
  const uploadedAt =
    value !== null && typeof value === "object" ? Reflect.get(value, "uploadedAt") : undefined;
  const modified =
    uploadedAt instanceof Date ? Option.getOrUndefined(DateTime.make(uploadedAt)) : undefined;
  return yield* Schema.decodeUnknownEffect(Schema.toType(ObjectMetadata))({
    ref: {
      storeId: `vercel:${connection.config.access}:${connection.config.storeId}`,
      key: observed.pathname,
    },
    ...(observed.size !== undefined && observed.size !== null ? { size: observed.size } : {}),
    ...(observed.etag ? { etag: observed.etag } : {}),
    ...(observed.contentType ? { contentType: observed.contentType } : {}),
    ...(observed.contentDisposition ? { contentDisposition: observed.contentDisposition } : {}),
    ...(observed.cacheControl ? { cacheControl: observed.cacheControl } : {}),
    ...(modified ? { lastModified: modified } : {}),
  }).pipe(
    Effect.mapError(() =>
      storageError(
        operation,
        "vercel",
        { _tag: "ProviderFailure" },
        mutation ? "unknown" : "not-applicable",
      ),
    ),
  );
});
export function providerError(operation: string, mutation: boolean, error: unknown) {
  const readStatus =
    error instanceof Blob.BlobError
      ? /^Vercel Blob: Failed to fetch blob: (\d{3}) /u.exec(error.message)?.[1]
      : undefined;
  const reason =
    readStatus === "401" || readStatus === "403"
      ? { _tag: "AccessDenied" as const }
      : readStatus === "412"
        ? { _tag: "PreconditionFailed" as const }
        : readStatus === "416"
          ? { _tag: "RangeNotSatisfiable" as const }
          : error instanceof Blob.BlobStoreNotFoundError
            ? { _tag: "InvalidConfiguration" as const, field: "store" }
            : error instanceof Blob.BlobNotFoundError
              ? { _tag: "NotFound" as const }
              : error instanceof Blob.BlobAccessError ||
                  error instanceof Blob.BlobStoreSuspendedError
                ? { _tag: "AccessDenied" as const }
                : error instanceof Blob.BlobClientTokenExpiredError
                  ? { _tag: "Expired" as const }
                  : error instanceof Blob.BlobPreconditionFailedError
                    ? { _tag: "PreconditionFailed" as const }
                    : error instanceof Blob.BlobServiceRateLimited
                      ? {
                          _tag: "Throttled" as const,
                          retryAfterMillis: Math.max(0, error.retryAfter * 1000),
                        }
                      : error instanceof Blob.BlobContentTypeNotAllowedError ||
                          error instanceof Blob.BlobPathnameMismatchError ||
                          error instanceof Blob.BlobFileTooLargeError
                        ? { _tag: "InvalidInput" as const, field: "upload" }
                        : { _tag: "TransportFailure" as const };
  const rejected = [
    "NotFound",
    "AccessDenied",
    "Expired",
    "PreconditionFailed",
    "InvalidInput",
    "Throttled",
  ].includes(reason._tag);
  return storageError(
    operation,
    "vercel",
    reason,
    mutation ? (rejected ? "rejected" : "unknown") : "not-applicable",
  );
}
