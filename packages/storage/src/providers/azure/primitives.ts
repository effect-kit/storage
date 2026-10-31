import { Readable } from "node:stream";

import { Effect, Stream } from "effect";

import { storageError } from "../../core/errors.js";
import { resolveRange } from "../../core/ranges.js";
import { Storage } from "../../core/storage.js";
import type { StorageService } from "../../core/storage.js";
import { decodeCursor, encodeCursor } from "../../internal/listing-cursor.js";
import { providerError } from "../../internal/node/provider-errors.js";
import { uploadBody } from "../../internal/node/upload-body.js";
import { encryption, lease } from "./config.js";
import type { AzureConnection } from "./config.js";
import { condition, metadata, nativeHeaders, target, writeMetadata } from "./mapping.js";
import { request } from "./transport.js";
import { validateKey } from "./validation.js";

export function primitives(connection: AzureConnection): StorageService {
  return Storage.make({
    provider: "azure",
    storeId: connection.config.storeId,
    capabilities: {
      putConditions: ["create-only", "if-match"],
      getConditions: ["if-match", "if-none-match"],
      deleteConditions: ["if-match"],
      ranges: ["bounded", "from", "suffix"],
      versionedReads: "history",
      list: { maxPageSize: 5000, delimiter: true, cursorPersistence: "provider", snapshot: false },
    },
    put: (input) =>
      validateKey(input.key, "put", true).pipe(
        Effect.andThen(writeMetadata(input)),
        Effect.flatMap((customMetadata) =>
          uploadBody(
            { provider: "azure", maxUnknownLengthBytes: connection.config.maxUnknownLengthBytes },
            {
              ...input,
              maxBytes: Math.min(input.maxBytes ?? 256 * 1024 * 1024, 256 * 1024 * 1024),
            },
            "put",
            (body, length) =>
              request(connection, "put", true, (signal) =>
                connection.client.getBlockBlobClient(input.key).upload(body, length, {
                  abortSignal: signal,
                  conditions: { ...condition(input.condition), ...lease(connection.config) },
                  blobHTTPHeaders: nativeHeaders(input),
                  metadata: customMetadata,
                  ...encryption(connection.config),
                  contentChecksumAlgorithm: connection.config.checksumAlgorithm,
                }),
              ).pipe(
                Effect.map((result) =>
                  metadata(connection, input.key, {
                    ...result,
                    contentLength: length,
                    ...input,
                    metadata: customMetadata,
                  }),
                ),
              ),
          ),
        ),
      ),
    head: Effect.fnUntraced(function* (input) {
      const address = yield* target(connection, input, "head");
      const output = yield* request(connection, "head", false, (signal) =>
        address.client.getProperties({
          abortSignal: signal,
          conditions: lease(connection.config),
          ...encryption(connection.config),
        }),
      );
      if (output.blobType !== "BlockBlob")
        return yield* storageError("head", "azure", {
          _tag: "Unsupported",
          capability: "block-blob-profile",
        });
      return metadata(connection, address.ref.key, output, address.ref);
    }),
    get: Effect.fnUntraced(function* (input) {
      const address = yield* target(connection, input, "get");
      if (input.range?._tag === "Bounded" && input.range.end < input.range.start)
        return yield* storageError("get", "azure", { _tag: "RangeNotSatisfiable" });
      if (input.condition?._tag === "IfNoneMatch" && input.condition.etag === "*") {
        const observed = yield* request(connection, "get", false, (signal) =>
          address.client.getProperties({
            abortSignal: signal,
            conditions: lease(connection.config),
            ...encryption(connection.config),
          }),
        );
        return {
          _tag: "NotModified" as const,
          metadata: metadata(connection, address.ref.key, observed, address.ref),
        };
      }
      let range = input.range;
      let pin: string | undefined;
      if (range?._tag === "Suffix") {
        const observed = yield* request(connection, "head", false, (signal) =>
          address.client.getProperties({
            abortSignal: signal,
            conditions: lease(connection.config),
            ...encryption(connection.config),
          }),
        );
        if (observed.contentLength === undefined || !observed.etag)
          return yield* storageError("get", "azure", { _tag: "ProviderFailure" });
        const resolved = yield* Effect.fromResult(
          resolveRange(range, observed.contentLength, "azure"),
        );
        range = { _tag: "Bounded", start: resolved.start, end: resolved.end };
        pin = observed.etag;
      }
      const output = yield* request(connection, "get", false, (signal) =>
        address.client.download(
          range ? range.start : undefined,
          range?._tag === "Bounded" ? range.end - range.start + 1 : undefined,
          {
            abortSignal: signal,
            maxRetryRequests: 0,
            contentChecksumAlgorithm: connection.config.checksumAlgorithm,
            conditions: {
              ...lease(connection.config),
              ...(input.condition
                ? input.condition._tag === "IfMatch"
                  ? { ifMatch: input.condition.etag }
                  : { ifNoneMatch: input.condition.etag }
                : {}),
              ...(pin ? { ifMatch: pin } : {}),
            },
            ...encryption(connection.config),
          },
        ),
      ).pipe(Effect.catchReason("StorageError", "NotModified", () => Effect.succeed(undefined)));
      if (!output)
        return {
          _tag: "NotModified" as const,
          metadata: { ref: address.ref, etag: input.condition?.etag },
        };
      // The SDK exposes the status through its public raw response property.
      // oxlint-disable-next-line no-underscore-dangle
      if (output._response.status === 304)
        return {
          _tag: "NotModified" as const,
          metadata: metadata(connection, address.ref.key, output, address.ref),
        };
      if (output.blobType !== "BlockBlob")
        return yield* storageError("get", "azure", {
          _tag: "Unsupported",
          capability: "block-blob-profile",
        });
      const body = output.readableStreamBody;
      if (!(body instanceof Readable))
        return yield* storageError("get", "azure", { _tag: "ProviderFailure" });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          body.destroy();
        }),
      );
      body.on("error", () => {});
      const match = /^bytes (\d+)-(\d+)\/(\d+)$/u.exec(output.contentRange ?? "");
      if (match && (Number(match[2]) < Number(match[1]) || Number(match[1]) >= Number(match[3])))
        return yield* storageError("get", "azure", { _tag: "RangeNotSatisfiable" });
      if (range && !match) return yield* storageError("get", "azure", { _tag: "ProviderFailure" });
      return {
        _tag: "Object" as const,
        metadata: {
          ...metadata(connection, address.ref.key, output, address.ref),
          size: match ? Number(match[3]) : output.contentLength,
          ...(match ? { checksums: undefined } : {}),
        },
        contentLength: output.contentLength,
        ...(match
          ? {
              range: {
                start: Number(match[1]),
                end: Number(match[2]),
                totalSize: Number(match[3]),
              },
            }
          : {}),
        body: Stream.fromAsyncIterable(body, (error) =>
          providerError("azure", "get", false, error),
        ),
      };
    }),
    delete: Effect.fnUntraced(function* (input) {
      const address = yield* target(connection, input, "delete");
      // No deleteSnapshots default: snapshots and versions must be selected explicitly.
      yield* request(connection, "delete", true, (signal) =>
        address.client.delete({
          abortSignal: signal,
          conditions: { ...condition(input.condition), ...lease(connection.config) },
        }),
      ).pipe(
        Effect.catch((error) =>
          error.reason._tag === "NotFound" && !input.condition ? Effect.void : Effect.fail(error),
        ),
      );
    }),
    listPage: Effect.fnUntraced(function* (input = {}) {
      const query = JSON.stringify([
        input.prefix ?? "",
        input.delimiter ?? null,
        input.limit ?? null,
      ]);
      const token = yield* decodeCursor(
        "azure",
        input.cursor,
        connection.config.storeId,
        query,
        "list",
      );
      const page = yield* request(connection, "list", false, async (signal) => {
        const pages = input.delimiter
          ? connection.client.listBlobsByHierarchy(input.delimiter, {
              ...(input.prefix !== undefined ? { prefix: input.prefix } : {}),
              includeMetadata: true,
              abortSignal: signal,
            })
          : connection.client.listBlobsFlat({
              ...(input.prefix !== undefined ? { prefix: input.prefix } : {}),
              includeMetadata: true,
              abortSignal: signal,
            });
        return await pages
          .byPage({
            ...(input.limit ? { maxPageSize: input.limit } : {}),
            ...(token ? { continuationToken: token } : {}),
          })
          .next();
      });
      if (page.done) return { entries: [] };
      const segment = page.value.segment;
      return {
        entries: [
          ...segment.blobItems.map((blob) => ({
            _tag: "Object" as const,
            metadata: metadata(connection, blob.name, {
              ...blob.properties,
              metadata: blob.metadata,
            }),
          })),
          ...(
            (page.value.segment as { blobPrefixes?: Array<{ name: string }> }).blobPrefixes ?? []
          ).map((prefix: { name: string }) => ({
            _tag: "CommonPrefix" as const,
            prefix: prefix.name,
          })),
        ],
        cursor: page.value.continuationToken
          ? encodeCursor(connection.config.storeId, query, page.value.continuationToken)
          : undefined,
      };
    }),
  });
}
