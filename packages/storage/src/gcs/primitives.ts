import { Effect, Predicate, Schema, Stream } from "effect";

import { storageError } from "../errors.js";
import { decodeCursor, encodeCursor } from "../internal/listing-cursor.js";
import { uploadBody } from "../internal/node/upload-body.js";
import { decodeRequest } from "../internal/request-validation.js";
import { ObjectSize } from "../object.js";
import { PutRequest as PutSchema } from "../requests.js";
import type { PutRequest } from "../requests.js";
import { Storage } from "../storage.js";
import type { StorageService } from "../storage.js";
import type { GcsConnection } from "./config.js";
import { metadata, NativeMetadata, target, responseMetadata } from "./mapping.js";
import { decodeJson, jsonRequest, mediaRequest } from "./transport.js";
import { multipartBody } from "./upload.js";
import { validateKey } from "./validation.js";

export const gcsCapabilities = {
  putConditions: ["create-only"],
  getConditions: ["if-match", "if-none-match"],
  deleteConditions: [],
  ranges: ["bounded", "from", "suffix"],
  versionedReads: "history",
  list: { maxPageSize: 1000, delimiter: true, cursorPersistence: "provider", snapshot: false },
} as const;

export const putObject = Effect.fnUntraced(function* <E, R>(
  connection: GcsConnection,
  request: PutRequest<E, R>,
  preconditions: Record<string, string | number | undefined> = {},
) {
  yield* decodeRequest(PutSchema, request, "put", "gcs", true);
  yield* validateKey(request.key, "put", true);
  // JSON API has generation preconditions rather than ETag write conditions.
  if (request.condition?._tag === "IfMatch")
    return yield* storageError(
      "put",
      "gcs",
      { _tag: "Unsupported", capability: "etag-write-condition" },
      "not-started",
    );
  const crc = connection.client.crc32cGenerator();
  const bytes = Predicate.isUint8Array(request.body);
  if (!bytes && !Stream.isStream(request.body))
    return yield* storageError(
      "put",
      "gcs",
      { _tag: "InvalidInput", field: "body" },
      "not-started",
    );
  const checksummedBody = Predicate.isUint8Array(request.body)
    ? request.body
    : Stream.tap(request.body, (chunk) =>
        Effect.sync(() =>
          crc.update(Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)),
        ),
      );
  if (Predicate.isUint8Array(checksummedBody))
    crc.update(
      Buffer.from(checksummedBody.buffer, checksummedBody.byteOffset, checksummedBody.byteLength),
    );
  return yield* uploadBody(
    { provider: "gcs", maxUnknownLengthBytes: connection.config.maxUnknownLengthBytes },
    {
      ...request,
      body: checksummedBody,
      maxBytes: Math.min(request.maxBytes ?? 256 * 1024 * 1024, 256 * 1024 * 1024),
    },
    "put",
    (body, length) =>
      Effect.gen(function* () {
        const multipart = multipartBody(
          request.key,
          request,
          body,
          length,
          bytes ? crc.toString() : undefined,
        );
        const response = yield* jsonRequest(connection, "put", true, {
          uri: `${connection.config.apiEndpoint}/upload/storage/v1/b/${encodeURIComponent(connection.config.bucket)}/o`,
          method: "POST",
          qs: {
            uploadType: "multipart",
            name: request.key,
            ...preconditions,
            ...(request.condition ? { ifGenerationMatch: "0" } : {}),
            ...(connection.config.kmsKeyName ? { kmsKeyName: connection.config.kmsKeyName } : {}),
          },
          body: multipart.body,
          headers: {
            "Content-Type": multipart.contentType,
            "Content-Length": String(multipart.length),
          },
          json: true,
        });
        const observed = yield* metadata(connection, response.body, "put");
        const expected = observed.checksums?.find((checksum) => checksum.algorithm === "crc32c");
        if (expected && !crc.validate(expected.value))
          return yield* storageError("put", "gcs", { _tag: "IntegrityMismatch" }, "succeeded");
        return observed;
      }),
  );
});

export const sessionUrl = Effect.fnUntraced(function* (connection: GcsConnection, value: unknown) {
  const url = typeof value === "string" ? URL.parse(value) : null;
  // Bearer handles are restricted to this configured authority, including custom endpoints.
  if (
    !url ||
    url.origin !== connection.config.apiEndpoint ||
    url.username ||
    url.password ||
    url.hash ||
    url.pathname !== `/upload/storage/v1/b/${encodeURIComponent(connection.config.bucket)}/o` ||
    url.searchParams.get("uploadType") !== "resumable" ||
    !url.searchParams.get("upload_id")
  )
    return yield* storageError("session", "gcs", { _tag: "InvalidSession" }, "not-started");
  return url.href;
});

export function primitives(connection: GcsConnection): StorageService {
  return Storage.make({
    provider: "gcs",
    storeId: connection.config.storeId,
    capabilities: gcsCapabilities,
    put: (request) => putObject(connection, request),
    head: Effect.fnUntraced(function* (request) {
      const address = yield* target(connection, request, "head");
      const response = yield* jsonRequest(connection, "head", false, {
        ...address,
        method: "GET",
        json: true,
      });
      return yield* metadata(connection, response.body, "head");
    }),
    get: Effect.fnUntraced(function* (request) {
      const address = yield* target(connection, request, "get");
      const range = request.range;
      const headers: Record<string, string> = {
        "Accept-Encoding": "gzip",
        "Cache-Control": "no-store",
      };
      if (range)
        headers.Range =
          range._tag === "Suffix"
            ? `bytes=-${range.length}`
            : `bytes=${range.start}-${range._tag === "Bounded" ? range.end : ""}`;
      if (request.condition)
        headers[request.condition._tag === "IfMatch" ? "If-Match" : "If-None-Match"] =
          request.condition.etag;
      const response = yield* mediaRequest(connection, {
        uri: address.uri.replace("/storage/v1/", "/download/storage/v1/"),
        qs: { ...address.qs, alt: "media" },
        headers,
      });
      const key = address.ref.key;
      if (response.status === 304)
        return {
          _tag: "NotModified" as const,
          metadata: { ref: address.ref, etag: response.headers.etag },
        };
      const observed = yield* metadata(connection, responseMetadata(response.headers, key), "get");
      const lastModified = response.headers["last-modified"]
        ? yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromHttpDate)(
            response.headers["last-modified"],
          ).pipe(Effect.mapError(() => storageError("get", "gcs", { _tag: "ProviderFailure" })))
        : undefined;
      const contentLength =
        response.headers["content-length"] === undefined
          ? undefined
          : yield* Schema.decodeUnknownEffect(ObjectSize)(
              Number(response.headers["content-length"]),
            ).pipe(Effect.mapError(() => storageError("get", "gcs", { _tag: "ProviderFailure" })));
      const match = /^bytes (\d+)-(\d+)\/(\d+)$/u.exec(response.headers["content-range"] ?? "");
      if (range && (!match || response.status !== 206))
        return yield* storageError("get", "gcs", { _tag: "ProviderFailure" });
      const expectedCrc = !range
        ? observed.checksums?.find((checksum) => checksum.algorithm === "crc32c")?.value
        : undefined;
      const crc = connection.client.crc32cGenerator();
      const body = expectedCrc
        ? response.body.pipe(
            Stream.tap((chunk) =>
              Effect.sync(() =>
                crc.update(Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)),
              ),
            ),
            Stream.concat(
              Stream.fromEffect(
                Effect.suspend(() =>
                  crc.validate(expectedCrc)
                    ? Effect.void
                    : Effect.fail(storageError("get", "gcs", { _tag: "IntegrityMismatch" })),
                ),
              ).pipe(Stream.drain),
            ),
          )
        : response.body;
      return {
        _tag: "Object" as const,
        metadata: { ...observed, ...(lastModified ? { lastModified } : {}) },
        contentLength,
        ...(match
          ? {
              range: {
                start: Number(match[1]),
                end: Number(match[2]),
                totalSize: Number(match[3]),
              },
            }
          : {}),
        body,
      };
    }),
    delete: Effect.fnUntraced(function* (request) {
      const address = yield* target(connection, request, "delete");
      yield* jsonRequest(connection, "delete", true, {
        uri: address.uri,
        qs: address.qs,
        method: "DELETE",
        headers: request.condition ? { "If-Match": request.condition.etag } : {},
      }).pipe(
        Effect.catch((error) =>
          error.reason._tag === "NotFound" && !request.condition ? Effect.void : Effect.fail(error),
        ),
      );
    }),
    listPage: Effect.fnUntraced(function* (request = {}) {
      const query = JSON.stringify([
        request.prefix ?? "",
        request.delimiter ?? null,
        request.limit ?? null,
      ]);
      const token = yield* decodeCursor(
        "gcs",
        request.cursor,
        connection.config.storeId,
        query,
        "list",
      );
      const response = yield* jsonRequest(connection, "list", false, {
        uri: `${connection.config.apiEndpoint}/storage/v1/b/${encodeURIComponent(connection.config.bucket)}/o`,
        method: "GET",
        json: true,
        qs: {
          prefix: request.prefix,
          delimiter: request.delimiter,
          maxResults: request.limit,
          pageToken: token,
        },
      });
      const page = yield* decodeJson(
        Schema.Struct({
          items: Schema.optional(Schema.Array(NativeMetadata)),
          prefixes: Schema.optional(Schema.Array(Schema.String)),
          nextPageToken: Schema.optional(Schema.String),
        }),
        response.body,
        "list",
      );
      const objects = yield* Effect.forEach(page.items ?? [], (entry) =>
        metadata(connection, entry, "list").pipe(
          Effect.map((value) => ({ _tag: "Object" as const, metadata: value })),
        ),
      );
      return {
        entries: [
          ...objects,
          ...(page.prefixes ?? []).map((prefix) => ({ _tag: "CommonPrefix" as const, prefix })),
        ],
        cursor: page.nextPageToken
          ? encodeCursor(connection.config.storeId, query, page.nextPageToken)
          : undefined,
      };
    }),
  });
}
