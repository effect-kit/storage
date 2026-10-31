import { Effect, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { storageError } from "../../core/errors.js";
import { ObjectKey, ObjectMetadata } from "../../core/object.js";
import type { GetRequest, GetResult, ObjectTarget, PutRequest } from "../../core/requests.js";
import { Storage } from "../../core/storage.js";
import { decodeCursor, encodeCursor } from "../../internal/listing-cursor.js";
import { readCondition } from "../../internal/read-condition.js";
import { webUploadBody } from "../../internal/web-upload-body.js";
import type { SupabaseConnection } from "./config.js";
import { fromHeaders, nativeHeaders, objectPath, validateKey } from "./mapping.js";
import { json, request } from "./transport.js";

const Info = Schema.Struct({
  name: ObjectKey,
  size: Schema.optional(Schema.NullOr(Schema.Natural)),
  etag: Schema.optional(Schema.NullOr(Schema.String)),
  content_type: Schema.optional(Schema.NullOr(Schema.String)),
  cache_control: Schema.optional(Schema.NullOr(Schema.String)),
  last_modified: Schema.optional(Schema.NullOr(Schema.String)),
  metadata: Schema.optional(Schema.NullOr(Schema.Record(Schema.String, Schema.Unknown))),
});
const listed = Schema.Struct({
  name: ObjectKey,
  key: Schema.optional(Schema.NullOr(Schema.String)),
  size: Schema.optional(Schema.NullOr(Schema.Natural)),
  metadata: Schema.optional(
    Schema.NullOr(Schema.Struct({ size: Schema.optional(Schema.Natural) })),
  ),
});
const Page = Schema.Struct({
  hasNext: Schema.Boolean,
  nextCursor: Schema.optional(Schema.NullOr(Schema.String)),
  objects: Schema.Array(listed),
  folders: Schema.Array(
    Schema.Struct({ name: ObjectKey, key: Schema.optional(Schema.NullOr(Schema.String)) }),
  ),
});

export const head = Effect.fnUntraced(function* (
  connection: SupabaseConnection,
  target: ObjectTarget,
) {
  const key = "ref" in target ? target.ref.key : target.key;
  yield* validateKey(key, "head");
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const response = yield* request(
        connection,
        "head",
        "GET",
        `/object/info/${objectPath(connection, key)}`,
      );
      const info = yield* json(response, Info, "head");
      if (info.name !== key)
        return yield* storageError("head", "supabase", { _tag: "ProviderFailure" });
      const metadata = yield* fromHeaders(
        connection,
        key,
        {
          ...(info.etag ? { etag: info.etag.startsWith('"') ? info.etag : `"${info.etag}"` } : {}),
          ...(info.content_type ? { "content-type": info.content_type } : {}),
          ...(info.cache_control ? { "cache-control": info.cache_control } : {}),
          ...(info.last_modified ? { "last-modified": info.last_modified } : {}),
        },
        info.size ?? undefined,
      );
      return yield* Schema.decodeUnknownEffect(Schema.toType(ObjectMetadata))({
        ...metadata,
        ...(info.metadata
          ? {
              customMetadata: Object.fromEntries(
                Object.entries(info.metadata).filter(
                  (entry): entry is [string, string] => typeof entry[1] === "string",
                ),
              ),
            }
          : {}),
      }).pipe(Effect.mapError(() => storageError("head", "supabase", { _tag: "ProviderFailure" })));
    }),
  );
});

export const put = Effect.fnUntraced(function* <E, R>(
  connection: SupabaseConnection,
  input: PutRequest<E, R>,
) {
  yield* validateKey(input.key, "put");
  const headers = yield* nativeHeaders(input, "put");
  return yield* webUploadBody(
    "supabase",
    { ...input, maxBytes: Math.min(input.maxBytes ?? Infinity, connection.config.maxObjectBytes) },
    connection.config.maxUnknownLengthBytes,
    (body, length) =>
      Effect.scoped(
        Effect.gen(function* () {
          const response = yield* request(
            connection,
            "put",
            "POST",
            `/object/${objectPath(connection, input.key)}`,
            {
              headers: { ...headers, "x-upsert": String(input.condition?._tag !== "CreateOnly") },
              body,
            },
          );
          // The upload acknowledgement does not contain an immutable version/validator. Do not race it with info().
          const acknowledgement = yield* json(
            response,
            Schema.Struct({
              Key: Schema.String,
              Id: Schema.optional(Schema.NullOr(Schema.String)),
            }),
            "put",
            true,
          );
          if (acknowledgement.Key !== `${connection.config.bucket}/${input.key}`)
            return yield* storageError("put", "supabase", { _tag: "IntegrityMismatch" }, "unknown");
          return {
            ref: { storeId: connection.config.storeId, key: input.key },
            size: length,
          } satisfies ObjectMetadata;
        }),
      ),
  );
});

export const get = Effect.fnUntraced(function* (
  connection: SupabaseConnection,
  input: GetRequest,
): Effect.fn.Return<GetResult, ReturnType<typeof storageError>, Scope.Scope> {
  const key = "ref" in input ? input.ref.key : input.key;
  yield* validateKey(key, "get");
  const range = input.range;
  const condition = input.condition;
  const headers = {
    ...(condition
      ? { [condition._tag === "IfMatch" ? "if-match" : "if-none-match"]: condition.etag }
      : {}),
    ...(range
      ? {
          range:
            range._tag === "Suffix"
              ? `bytes=-${range.length}`
              : `bytes=${range.start}-${range._tag === "Bounded" ? range.end : ""}`,
        }
      : {}),
  };
  const response = yield* request(
    connection,
    "get",
    "GET",
    `/object/${objectPath(connection, key)}`,
    { headers, accepted: [304] },
  );
  let resolved: { start: number; end: number; totalSize: number } | undefined;
  if (range && response.status !== 304) {
    const match = /^bytes (\d+)-(\d+)\/(\d+)$/u.exec(response.headers["content-range"] ?? "");
    if (response.status !== 206 || !match)
      return yield* storageError("get", "supabase", {
        _tag: "Unsupported",
        capability: "range-response",
      });
    resolved = { start: Number(match[1]), end: Number(match[2]), totalSize: Number(match[3]) };
    const expectedStart =
      range._tag === "Suffix" ? Math.max(0, resolved.totalSize - range.length) : range.start;
    const expectedEnd =
      range._tag === "Bounded"
        ? Math.min(range.end, resolved.totalSize - 1)
        : resolved.totalSize - 1;
    if (
      resolved.start !== expectedStart ||
      resolved.end !== expectedEnd ||
      !Object.values(resolved).every(Number.isSafeInteger)
    )
      return yield* storageError("get", "supabase", { _tag: "ProviderFailure" });
  }
  if (response.headers["content-encoding"] && response.headers["content-encoding"] !== "identity")
    return yield* storageError("get", "supabase", {
      _tag: "Unsupported",
      capability: "encoded-representation",
    });
  if (response.status === 304 && condition?._tag !== "IfNoneMatch")
    return yield* storageError("get", "supabase", { _tag: "ProviderFailure" });
  const metadata = yield* fromHeaders(connection, key, response.headers, resolved?.totalSize);
  // Same-response checks preserve conditions on deployments that ignore request validators.
  const outcome = readCondition(condition, metadata.etag);
  if (outcome === "failed")
    return yield* storageError("get", "supabase", { _tag: "PreconditionFailed" });
  if (response.status === 304 || outcome === "not-modified")
    return { _tag: "NotModified", metadata };
  return {
    _tag: "Object",
    metadata,
    contentLength:
      response.headers["content-length"] === undefined
        ? undefined
        : Number(response.headers["content-length"]),
    ...(resolved ? { range: resolved } : {}),
    body: Stream.mapError(response.stream, () =>
      storageError("get", "supabase", { _tag: "TransportFailure" }),
    ),
  };
});

export function primitives(connection: SupabaseConnection) {
  return Storage.make({
    provider: "supabase",
    storeId: connection.config.storeId,
    capabilities: {
      putConditions: ["create-only"],
      getConditions: ["if-match", "if-none-match"],
      deleteConditions: [],
      ranges: ["bounded", "from", "suffix"],
      versionedReads: "none",
      list: { maxPageSize: 1000, delimiter: true, cursorPersistence: "provider", snapshot: false },
    },
    put: (input) => put(connection, input),
    get: (input) => get(connection, input),
    head: (input) => head(connection, input),
    delete: Effect.fnUntraced(function* (input) {
      const key = "ref" in input ? input.ref.key : input.key;
      yield* validateKey(key, "delete");
      yield* Effect.scoped(
        Effect.gen(function* () {
          const response = yield* request(
            connection,
            "delete",
            "DELETE",
            `/object/${encodeURIComponent(connection.config.bucket)}`,
            { json: { prefixes: [key] } },
          );
          yield* json(response, Schema.Array(Schema.Unknown), "delete", true);
        }),
      );
    }),
    listPage: Effect.fnUntraced(function* (input = {}) {
      const query = JSON.stringify([
        input.prefix ?? "",
        input.delimiter ?? null,
        input.limit ?? null,
      ]);
      const token = yield* decodeCursor(
        "supabase",
        input.cursor,
        connection.config.storeId,
        query,
        "list",
      );
      if (input.delimiter && input.delimiter !== "/")
        return yield* storageError("list", "supabase", {
          _tag: "Unsupported",
          capability: "delimiter",
        });
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const response = yield* request(
            connection,
            "list",
            "POST",
            `/object/list-v2/${encodeURIComponent(connection.config.bucket)}`,
            {
              json: {
                prefix: input.prefix ?? "",
                limit: Math.min(input.limit ?? 1000, 1000),
                ...(token ? { cursor: token } : {}),
                with_delimiter: Boolean(input.delimiter),
              },
            },
          );
          const page = yield* json(response, Page, "list");
          if (page.hasNext && !page.nextCursor)
            return yield* storageError("list", "supabase", { _tag: "ProviderFailure" });
          return {
            entries: [
              ...page.objects.map((entry) => ({
                _tag: "Object" as const,
                metadata: {
                  ref: { storeId: connection.config.storeId, key: entry.name },
                  ...(entry.metadata?.size !== undefined ? { size: entry.metadata.size } : {}),
                },
              })),
              ...page.folders.map((folder) => ({
                _tag: "CommonPrefix" as const,
                prefix: folder.name,
              })),
            ],
            ...(page.hasNext
              ? { cursor: encodeCursor(connection.config.storeId, query, page.nextCursor ?? "") }
              : {}),
          };
        }),
      );
    }),
  });
}
