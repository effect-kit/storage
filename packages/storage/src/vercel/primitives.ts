import { Effect, Stream } from "effect";
import type { Scope } from "effect";

import * as Blob from "@vercel/blob";

import { storageError } from "../errors.js";
import { decodeCursor, encodeCursor } from "../internal/listing-cursor.js";
import { readCondition } from "../internal/read-condition.js";
import { webUploadBody } from "../internal/web-upload-body.js";
import type { GetRequest, GetResult, PutRequest } from "../requests.js";
import { Storage } from "../storage.js";
import type { VercelConnection } from "./config.js";
import { metadata, providerError, validateKey, writeOptions } from "./mapping.js";
import { call } from "./transport.js";

export const get = Effect.fnUntraced(function* (
  connection: VercelConnection,
  input: GetRequest,
): Effect.fn.Return<GetResult, ReturnType<typeof storageError>, Scope.Scope> {
  const key = "ref" in input ? input.ref.key : input.key;
  yield* validateKey(key, "get");
  const controller = yield* Effect.acquireRelease(
    Effect.sync(() => new AbortController()),
    (owned) => Effect.sync(() => owned.abort()),
  );
  const range = input.range;
  const response = yield* call(connection, "get", false, (options) =>
    Blob.get(
      `${connection.config.apiOrigin ?? `https://${connection.config.storeId}.${connection.config.access}.blob.vercel-storage.com`}/${key.split("/").map(encodeURIComponent).join("/")}`,
      {
        ...options,
        abortSignal: AbortSignal.any([options.abortSignal, controller.signal]),
        access: connection.config.access,
        useCache: false,
        ...(input.condition?._tag === "IfNoneMatch" ? { ifNoneMatch: input.condition.etag } : {}),
        headers: {
          ...(input.condition?._tag === "IfMatch" ? { "if-match": input.condition.etag } : {}),
          ...(range
            ? {
                range:
                  range._tag === "Suffix"
                    ? `bytes=-${range.length}`
                    : `bytes=${range.start}-${range._tag === "Bounded" ? range.end : ""}`,
              }
            : {}),
        },
      },
    ),
  );
  if (!response) return yield* storageError("get", "vercel", { _tag: "NotFound" });
  if (response.stream)
    yield* Effect.addFinalizer(() =>
      Effect.promise(() => response.stream?.cancel().catch(() => undefined) ?? Promise.resolve()),
    );
  if (
    response.headers.get("content-encoding") &&
    response.headers.get("content-encoding") !== "identity"
  )
    return yield* storageError("get", "vercel", {
      _tag: "Unsupported",
      capability: "encoded-representation",
    });
  if (response.statusCode === 304 && input.condition?._tag !== "IfNoneMatch")
    return yield* storageError("get", "vercel", { _tag: "ProviderFailure" });
  let observed = yield* metadata(connection, { ...response.blob, pathname: key }, "get", key);
  const outcome = readCondition(input.condition, observed.etag);
  if (outcome === "failed")
    return yield* storageError("get", "vercel", { _tag: "PreconditionFailed" });
  if (response.statusCode === 304 || outcome === "not-modified")
    return { _tag: "NotModified", metadata: observed };
  let resolved: { start: number; end: number; totalSize: number } | undefined;
  if (range) {
    const match = /^bytes (\d+)-(\d+)\/(\d+)$/u.exec(response.headers.get("content-range") ?? "");
    if (!match)
      return yield* storageError("get", "vercel", {
        _tag: "Unsupported",
        capability: "range-response",
      });
    resolved = { start: Number(match[1]), end: Number(match[2]), totalSize: Number(match[3]) };
    const start =
      range._tag === "Suffix" ? Math.max(0, resolved.totalSize - range.length) : range.start;
    const end =
      range._tag === "Bounded"
        ? Math.min(range.end, resolved.totalSize - 1)
        : resolved.totalSize - 1;
    if (
      resolved.start !== start ||
      resolved.end !== end ||
      !Object.values(resolved).every(Number.isSafeInteger)
    )
      return yield* storageError("get", "vercel", { _tag: "ProviderFailure" });
    observed = { ...observed, size: resolved.totalSize };
  }
  return {
    _tag: "Object",
    metadata: observed,
    contentLength: resolved ? resolved.end - resolved.start + 1 : observed.size,
    ...(resolved ? { range: resolved } : {}),
    body: Stream.fromReadableStream({
      evaluate: () => response.stream,
      onError: () => providerError("get", false, {}),
    }),
  };
});

export const put = Effect.fnUntraced(function* <E, R>(
  connection: VercelConnection,
  input: PutRequest<E, R>,
) {
  yield* validateKey(input.key, "put");
  const settings = yield* writeOptions(input, "put");
  return yield* webUploadBody(
    "vercel",
    { ...input, maxBytes: Math.min(input.maxBytes ?? Infinity, connection.config.maxObjectBytes) },
    connection.config.maxUnknownLengthBytes,
    (body, length) =>
      call(connection, "put", true, (options) =>
        Blob.put(
          input.key,
          body instanceof Uint8Array
            ? new ReadableStream({
                start(controller) {
                  controller.enqueue(body);
                  controller.close();
                },
              })
            : body,
          {
            ...options,
            ...settings,
            access: connection.config.access,
            multipart: false,
            allowOverwrite: input.condition?._tag !== "CreateOnly",
            ...(input.condition?._tag === "IfMatch" ? { ifMatch: input.condition.etag } : {}),
          },
        ),
      ).pipe(
        Effect.flatMap((result) => metadata(connection, result, "put", input.key, true)),
        Effect.map((result) => ({ ...result, size: length })),
      ),
  );
});

export function primitives(connection: VercelConnection) {
  return Storage.make({
    provider: "vercel",
    storeId: `vercel:${connection.config.access}:${connection.config.storeId}`,
    capabilities: {
      putConditions: ["create-only", "if-match"],
      getConditions: ["if-match", "if-none-match"],
      deleteConditions: ["if-match"],
      ranges: ["bounded", "from", "suffix"],
      versionedReads: "none",
      list: { maxPageSize: 1000, delimiter: true, cursorPersistence: "provider", snapshot: false },
    },
    put: (input) => put(connection, input),
    get: (input) => get(connection, input),
    head: Effect.fnUntraced(function* (input) {
      const key = "ref" in input ? input.ref.key : input.key;
      yield* validateKey(key, "head");
      return yield* call(connection, "head", false, (options) => Blob.head(key, options)).pipe(
        Effect.flatMap((result) => metadata(connection, result, "head", key)),
      );
    }),
    delete: Effect.fnUntraced(function* (input) {
      const key = "ref" in input ? input.ref.key : input.key;
      yield* validateKey(key, "delete");
      yield* call(connection, "delete", true, (options) =>
        Blob.del(key, {
          ...options,
          ...(input.condition ? { ifMatch: input.condition.etag } : {}),
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
        "vercel",
        input.cursor,
        `vercel:${connection.config.access}:${connection.config.storeId}`,
        query,
        "list",
      );
      if (input.delimiter && input.delimiter !== "/")
        return yield* storageError("list", "vercel", {
          _tag: "Unsupported",
          capability: "delimiter",
        });
      const result = yield* call(connection, "list", false, (options) =>
        Blob.list({
          ...options,
          prefix: input.prefix ?? "",
          limit: Math.min(input.limit ?? 1000, 1000),
          ...(token ? { cursor: token } : {}),
          mode: input.delimiter ? "folded" : "expanded",
        }),
      );
      if (result.hasMore && !result.cursor)
        return yield* storageError("list", "vercel", { _tag: "ProviderFailure" });
      const entries = yield* Effect.forEach(result.blobs, (blob) =>
        metadata(connection, blob, "list").pipe(
          Effect.map((value) => ({ _tag: "Object" as const, metadata: value })),
        ),
      );
      return {
        entries: [
          ...entries,
          ...("folders" in result && Array.isArray(result.folders)
            ? result.folders.map((prefix: string) => ({ _tag: "CommonPrefix" as const, prefix }))
            : []),
        ],
        ...(result.hasMore
          ? {
              cursor: encodeCursor(
                `vercel:${connection.config.access}:${connection.config.storeId}`,
                query,
                result.cursor ?? "",
              ),
            }
          : {}),
      };
    }),
  });
}
