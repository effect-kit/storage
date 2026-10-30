import { DateTime, Effect, Option, Schema } from "effect";
import { Base64 } from "effect/encoding";

import { storageError } from "../errors.js";
import { ObjectKey } from "../object.js";
import type { ObjectMetadata, ObjectRef, ObjectHeaders } from "../object.js";
import type { SupabaseConnection } from "./config.js";

export const validateKey = Effect.fnUntraced(function* (key: string, operation: string) {
  yield* Schema.decodeUnknownEffect(ObjectKey)(key).pipe(
    Effect.mapError(() =>
      storageError(operation, "supabase", { _tag: "InvalidInput", field: "key" }, "not-started"),
    ),
  );
  if (
    key.split("/").some((part) => !part || part === "." || part === "..") ||
    new TextEncoder().encode(key).length > 1024
  )
    return yield* storageError(
      operation,
      "supabase",
      { _tag: "InvalidInput", field: "key" },
      "not-started",
    );
  return key;
});
export const objectPath = (connection: SupabaseConnection, key: string) =>
  `${encodeURIComponent(connection.config.bucket)}/${key.split("/").map(encodeURIComponent).join("/")}`;
export const validateRef = Effect.fnUntraced(function* (
  connection: SupabaseConnection,
  ref: ObjectRef,
  operation: string,
) {
  if (ref.storeId !== connection.config.storeId || ref.version)
    return yield* storageError(
      operation,
      "supabase",
      { _tag: "Unsupported", capability: "foreign-or-versioned-reference" },
      "not-started",
    );
  return yield* validateKey(ref.key, operation);
});
export const nativeHeaders = Effect.fnUntraced(function* (
  headers: ObjectHeaders,
  operation: string,
) {
  if (headers.contentDisposition || headers.contentLanguage || headers.contentEncoding)
    return yield* storageError(
      operation,
      "supabase",
      { _tag: "Unsupported", capability: "encoded-content-or-disposition-or-language" },
      "not-started",
    );
  const result = {
    "content-type": headers.contentType ?? "application/octet-stream",
    ...(headers.cacheControl ? { "cache-control": headers.cacheControl } : {}),
    ...(headers.contentEncoding ? { "content-encoding": headers.contentEncoding } : {}),
    ...(headers.customMetadata
      ? {
          "x-metadata": Base64.encode(
            new TextEncoder().encode(JSON.stringify(headers.customMetadata)),
          ),
        }
      : {}),
  };
  if (new TextEncoder().encode(JSON.stringify(result)).length > 8192)
    return yield* storageError(
      operation,
      "supabase",
      { _tag: "InvalidInput", field: "headers" },
      "not-started",
    );
  return result;
});
export const fromHeaders = Effect.fnUntraced(function* (
  connection: SupabaseConnection,
  key: string,
  headers: Readonly<Record<string, string>>,
  size?: number,
) {
  const metadata: ObjectMetadata = {
    ref: { storeId: connection.config.storeId, key },
    ...(size !== undefined
      ? { size }
      : headers["content-length"] !== undefined
        ? { size: Number(headers["content-length"]) }
        : {}),
    ...(headers.etag ? { etag: headers.etag } : {}),
    ...(headers["content-type"] ? { contentType: headers["content-type"] } : {}),
    ...(headers["cache-control"] ? { cacheControl: headers["cache-control"] } : {}),
    ...(headers["last-modified"]
      ? { lastModified: Option.getOrUndefined(DateTime.make(headers["last-modified"])) }
      : {}),
  };
  if (metadata.size !== undefined && (!Number.isSafeInteger(metadata.size) || metadata.size < 0))
    return yield* storageError("metadata", "supabase", { _tag: "ProviderFailure" });
  return metadata;
});
