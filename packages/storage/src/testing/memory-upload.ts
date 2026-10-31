import { DateTime, Effect, Predicate, Ref, Result, Stream } from "effect";

import { storageError } from "../core/errors.js";
import type { StorageError } from "../core/errors.js";
import type { ObjectHeaders, ObjectMetadata } from "../core/object.js";
import type { PutRequest } from "../core/requests.js";
import { cloneMetadata } from "./memory-state.js";
import type { MemoryContext, MemoryState, StoredObject } from "./memory-state.js";

export const putMemory = Effect.fnUntraced(function* <E = never, R = never>(
  context: MemoryContext,
  request: PutRequest<E, R>,
): Effect.fn.Return<ObjectMetadata, StorageError | E, R> {
  const { limits, state, storeId } = context;
  const headers: ObjectHeaders = {
    contentType: request.contentType,
    contentEncoding: request.contentEncoding,
    contentLanguage: request.contentLanguage,
    contentDisposition: request.contentDisposition,
    cacheControl: request.cacheControl,
    customMetadata: request.customMetadata ? { ...request.customMetadata } : undefined,
  };

  const limit = Math.min(limits.maxObjectBytes, request.maxBytes ?? limits.maxObjectBytes);
  if (new TextEncoder().encode(request.key).length > limits.maxKeyBytes) {
    return yield* storageError(
      "put",
      "memory",
      { _tag: "InvalidInput", field: "key" },
      "not-started",
    );
  }
  if (new TextEncoder().encode(JSON.stringify(headers)).length > limits.maxMetadataBytes) {
    return yield* storageError(
      "put",
      "memory",
      { _tag: "InvalidInput", field: "metadata" },
      "not-started",
    );
  }
  if (request.contentLength !== undefined && request.contentLength > limit) {
    return yield* storageError("put", "memory", { _tag: "LimitExceeded", limit }, "not-started");
  }

  return yield* Effect.acquireUseRelease(
    Effect.sync(() => ({ chunks: [] as Uint8Array[], length: 0, reserved: 0 })),
    (staging) =>
      Effect.gen(function* () {
        const source = Predicate.isUint8Array(request.body)
          ? Stream.succeed(request.body)
          : request.body;
        yield* Stream.runForEach(source, (chunk) =>
          Effect.gen(function* () {
            if (!Predicate.isUint8Array(chunk)) {
              return yield* storageError(
                "put",
                "memory",
                { _tag: "InvalidInput", field: "body" },
                "rejected",
              );
            }
            if (chunk.length > limit - staging.length) {
              return yield* storageError(
                "put",
                "memory",
                { _tag: "LimitExceeded", limit },
                "rejected",
              );
            }

            for (let offset = 0; offset < chunk.length; offset += limits.chunkSize) {
              const length = Math.min(limits.chunkSize, chunk.length - offset);
              const reserved = yield* Ref.modify(state, (current) => {
                // Reserve both staged copies and final concatenation before retaining bytes.
                if (length > Math.floor((limits.maxPendingBytes - current.pendingBytes) / 2)) {
                  return [false, current] as const;
                }
                staging.reserved += length * 2;
                return [
                  true,
                  { ...current, pendingBytes: current.pendingBytes + length * 2 },
                ] as const;
              });

              if (!reserved) {
                return yield* storageError(
                  "put",
                  "memory",
                  { _tag: "QuotaExceeded", limit: limits.maxPendingBytes },
                  "rejected",
                );
              }
              staging.chunks.push(Uint8Array.from(chunk.subarray(offset, offset + length)));
              staging.length += length;
            }
          }),
        );

        if (request.contentLength !== undefined && request.contentLength !== staging.length) {
          return yield* storageError(
            "put",
            "memory",
            {
              _tag: "LengthMismatch",
              expected: request.contentLength,
              actual: staging.length,
            },
            "rejected",
          );
        }

        const bytes = new Uint8Array(staging.length);
        let offset = 0;
        for (const chunk of staging.chunks) {
          bytes.set(chunk, offset);
          offset += chunk.length;
        }

        const now = yield* DateTime.now;
        const committed = yield* Ref.modify(
          state,
          (current): readonly [Result.Result<ObjectMetadata, StorageError>, MemoryState] => {
            const previous = current.objects.get(request.key);
            const rejected = (error: StorageError) => [Result.fail(error), current] as const;
            if (
              (request.condition?._tag === "CreateOnly" && previous !== undefined) ||
              (request.condition?._tag === "IfMatch" &&
                (previous === undefined ||
                  (request.condition.etag !== "*" &&
                    previous.metadata.etag !== request.condition.etag)))
            ) {
              return rejected(
                storageError("put", "memory", { _tag: "PreconditionFailed" }, "rejected"),
              );
            }

            const retained = current.totalBytes - (previous?.bytes.length ?? 0);
            if (bytes.length > limits.maxTotalBytes - retained) {
              return rejected(
                storageError(
                  "put",
                  "memory",
                  { _tag: "QuotaExceeded", limit: limits.maxTotalBytes },
                  "rejected",
                ),
              );
            }
            if (previous === undefined && current.objects.size >= limits.maxObjects) {
              return rejected(
                storageError(
                  "put",
                  "memory",
                  { _tag: "QuotaExceeded", limit: limits.maxObjects },
                  "rejected",
                ),
              );
            }

            const revision = current.revision + 1n;
            const metadata: ObjectMetadata = {
              ...headers,
              ref: {
                storeId,
                key: request.key,
                version: {
                  _tag: "OpaqueVersion",
                  provider: "memory",
                  id: `${context.instanceId}:${String(revision)}`,
                },
              },
              size: bytes.length,
              etag: `"memory-${String(revision)}"`,
              lastModified: now,
            };
            const objects = new Map(current.objects);
            const entry: StoredObject = { metadata, bytes };
            objects.set(request.key, entry);

            return [
              Result.succeed(cloneMetadata(metadata)),
              {
                ...current,
                objects,
                revision,
                totalBytes: retained + bytes.length,
              },
            ] as const;
          },
        );

        return yield* Effect.fromResult(committed);
      }),
    (staging) =>
      Ref.update(state, (current) => ({
        ...current,
        pendingBytes: current.pendingBytes - staging.reserved,
      })),
  );
});
