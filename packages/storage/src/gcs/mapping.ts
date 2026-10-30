import { Effect, Schema } from "effect";

import { storageError } from "../errors.js";
import { ObjectMetadata, ObjectRef } from "../object.js";
import type { ObjectHeaders } from "../object.js";
import type { ObjectTarget } from "../requests.js";
import type { GcsConnection } from "./config.js";
import { decodeJson } from "./transport.js";
import { validateKey } from "./validation.js";

export const NativeMetadata = Schema.Struct({
  name: Schema.NonEmptyString,
  generation: Schema.NonEmptyString.check(Schema.isPattern(/^\d+$/u)),
  metageneration: Schema.optional(Schema.NonEmptyString),
  size: Schema.optional(Schema.String),
  etag: Schema.optional(Schema.String),
  updated: Schema.optional(Schema.String),
  contentType: Schema.optional(Schema.String),
  contentEncoding: Schema.optional(Schema.String),
  contentLanguage: Schema.optional(Schema.String),
  contentDisposition: Schema.optional(Schema.String),
  cacheControl: Schema.optional(Schema.String),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  crc32c: Schema.optional(Schema.String),
  md5Hash: Schema.optional(Schema.String),
  storageClass: Schema.optional(Schema.String),
});
export const metadata = Effect.fnUntraced(function* (
  connection: GcsConnection,
  raw: unknown,
  operation: string,
) {
  const value = yield* decodeJson(NativeMetadata, raw, operation);
  return yield* decodeJson(
    ObjectMetadata,
    {
      ref: {
        storeId: connection.config.storeId,
        key: value.name,
        version: {
          _tag: "GcsGeneration",
          id: value.generation,
          metageneration: value.metageneration,
        },
      },
      size: value.size === undefined ? undefined : Number(value.size),
      etag:
        value.etag === undefined
          ? undefined
          : /^(?:W\/)?"/u.test(value.etag)
            ? value.etag
            : `"${value.etag}"`,
      lastModified: value.updated,
      contentType: value.contentType,
      contentEncoding: value.contentEncoding,
      contentLanguage: value.contentLanguage,
      contentDisposition: value.contentDisposition,
      cacheControl: value.cacheControl,
      customMetadata: value.metadata,
      storageClass: value.storageClass,
      checksums: [
        ...(value.crc32c
          ? [{ algorithm: "crc32c", encoding: "base64", scope: "full-object", value: value.crc32c }]
          : []),
        ...(value.md5Hash
          ? [{ algorithm: "md5", encoding: "base64", scope: "full-object", value: value.md5Hash }]
          : []),
      ],
    },
    operation,
  );
});
export function objectPath(connection: GcsConnection, key: string) {
  return `/storage/v1/b/${encodeURIComponent(connection.config.bucket)}/o/${encodeURIComponent(key)}`;
}
export const target = Effect.fnUntraced(function* (
  connection: GcsConnection,
  input: ObjectTarget,
  operation: string,
) {
  const ref = "ref" in input ? input.ref : { storeId: connection.config.storeId, key: input.key };
  yield* Schema.decodeUnknownEffect(ObjectRef)(ref).pipe(
    Effect.mapError(() => storageError(operation, "gcs", { _tag: "InvalidInput", field: "ref" })),
  );
  if (
    ref.storeId !== connection.config.storeId ||
    (ref.version && ref.version._tag !== "GcsGeneration")
  )
    return yield* storageError(operation, "gcs", { _tag: "InvalidInput", field: "ref" });
  yield* validateKey(ref.key, operation, operation === "delete");
  return {
    ref,
    uri: connection.config.apiEndpoint + objectPath(connection, ref.key),
    qs: ref.version ? { generation: ref.version.id } : {},
  };
});
export function nativeHeaders(input: ObjectHeaders) {
  return {
    contentType: input.contentType,
    contentEncoding: input.contentEncoding,
    contentLanguage: input.contentLanguage,
    contentDisposition: input.contentDisposition,
    cacheControl: input.cacheControl,
    metadata: input.customMetadata,
  };
}
export function responseMetadata(headers: Record<string, string>, key: string) {
  const custom = Object.fromEntries(
    Object.entries(headers)
      .filter(([name]) => name.startsWith("x-goog-meta-"))
      .map(([name, value]) => [name.slice(12), value]),
  );
  const hash = Object.fromEntries(
    (headers["x-goog-hash"] ?? "").split(",").map((part) => {
      const index = part.indexOf("=");
      return [part.slice(0, index).trim(), part.slice(index + 1)];
    }),
  );
  const range = /^bytes (\d+)-(\d+)\/(\d+)$/u.exec(headers["content-range"] ?? "");
  return {
    name: key,
    generation: headers["x-goog-generation"],
    metageneration: headers["x-goog-metageneration"],
    size: range?.[3] ?? headers["x-goog-stored-content-length"] ?? headers["content-length"],
    etag: headers.etag,
    contentType: headers["content-type"],
    contentEncoding: headers["content-encoding"],
    contentLanguage: headers["content-language"],
    contentDisposition: headers["content-disposition"],
    cacheControl: headers["cache-control"],
    metadata: custom,
    crc32c: hash.crc32c,
    md5Hash: hash.md5,
  };
}
