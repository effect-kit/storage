import { Context, Effect, Layer, Random, Ref, Result, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { storageError } from "../core/errors.js";
import type { StorageError } from "../core/errors.js";
import { resolveRange } from "../core/ranges.js";
import type { DeleteRequest, GetRequest, GetResult, HeadRequest } from "../core/requests.js";
import { Storage } from "../core/storage.js";
import { TransferDriver } from "../transfers/dependencies.js";
import { listMemory } from "./memory-list.js";
import {
  MemoryOptions,
  cloneMetadata,
  findObject,
  matchesVersion,
  memoryCapabilities,
} from "./memory-state.js";
import type { MemoryContext, MemoryState } from "./memory-state.js";
import { putMemory } from "./memory-upload.js";

export type { MemoryOptions } from "./memory-state.js";

const getMemory = Effect.fnUntraced(function* (
  context: MemoryContext,
  request: GetRequest,
): Effect.fn.Return<GetResult, StorageError, Scope.Scope> {
  const current = yield* Ref.get(context.state);
  const entry = yield* Effect.fromResult(findObject(current, request, "get"));
  const metadata = cloneMetadata(entry.metadata);

  if (
    request.condition?._tag === "IfMatch" &&
    request.condition.etag !== "*" &&
    request.condition.etag !== metadata.etag
  ) {
    return yield* storageError("get", "memory", { _tag: "PreconditionFailed" });
  }
  if (
    request.condition?._tag === "IfNoneMatch" &&
    (request.condition.etag === "*" || request.condition.etag === metadata.etag)
  ) {
    return { _tag: "NotModified", metadata };
  }

  const range = request.range
    ? yield* Effect.fromResult(resolveRange(request.range, entry.bytes.length, "memory"))
    : undefined;

  yield* Effect.acquireRelease(
    Ref.modify(context.state, (state) => {
      if (entry.bytes.length > context.limits.maxReadBytes - state.activeReadBytes) {
        return [false, state] as const;
      }
      return [
        true,
        { ...state, activeReadBytes: state.activeReadBytes + entry.bytes.length },
      ] as const;
    }).pipe(
      Effect.flatMap((reserved) =>
        reserved
          ? Effect.void
          : Effect.fail(
              storageError("get", "memory", {
                _tag: "QuotaExceeded",
                limit: context.limits.maxReadBytes,
              }),
            ),
      ),
    ),
    () =>
      Ref.update(context.state, (state) => ({
        ...state,
        activeReadBytes: state.activeReadBytes - entry.bytes.length,
      })),
  );

  const start = range?.start ?? 0;
  const end = range?.end ?? entry.bytes.length - 1;
  const chunks = {
    *[Symbol.iterator]() {
      for (let offset = start; offset <= end; offset += context.limits.chunkSize) {
        yield entry.bytes.slice(offset, Math.min(end + 1, offset + context.limits.chunkSize));
      }
    },
  };

  return {
    _tag: "Object",
    metadata,
    contentLength: Math.max(0, end - start + 1),
    range,
    body: Stream.fromIterable(chunks, { chunkSize: 1 }),
  };
});

const deleteMemory = Effect.fnUntraced(function* (context: MemoryContext, request: DeleteRequest) {
  const outcome = yield* Ref.modify(
    context.state,
    (state): readonly [Result.Result<void, StorageError>, MemoryState] => {
      const key = "ref" in request ? request.ref.key : request.key;
      const candidate = state.objects.get(key);
      const entry = candidate && matchesVersion(candidate, request) ? candidate : undefined;
      if (
        request.condition &&
        (entry === undefined ||
          (request.condition.etag !== "*" && entry.metadata.etag !== request.condition.etag))
      ) {
        return [
          Result.fail(storageError("delete", "memory", { _tag: "PreconditionFailed" }, "rejected")),
          state,
        ] as const;
      }

      if (entry === undefined) return [Result.succeed(undefined), state] as const;
      const objects = new Map(state.objects);
      objects.delete(key);

      return [
        Result.succeed(undefined),
        { ...state, objects, totalBytes: state.totalBytes - entry.bytes.length },
      ] as const;
    },
  );

  yield* Effect.fromResult(outcome);
});

export const make = Effect.fnUntraced(function* (options: MemoryOptions = {}) {
  const input = yield* Schema.decodeUnknownEffect(MemoryOptions, { onExcessProperty: "error" })(
    options,
  ).pipe(
    Effect.mapError(() =>
      storageError("make", "memory", { _tag: "InvalidConfiguration", field: "options" }),
    ),
  );

  const storeId =
    input.storeId ?? `memory:${String(yield* Random.nextInt)}:${String(yield* Random.nextInt)}`;
  const instanceId = `${String(yield* Random.nextInt)}:${String(yield* Random.nextInt)}`;
  const state = yield* Ref.make<MemoryState>({
    objects: new Map(),
    revision: 0n,
    totalBytes: 0,
    pendingBytes: 0,
    activeReadBytes: 0,
  });
  const context: MemoryContext = {
    storeId,
    instanceId,
    state,
    limits: {
      maxObjectBytes: input.maxObjectBytes ?? 16 * 1024 * 1024,
      maxTotalBytes: input.maxTotalBytes ?? 64 * 1024 * 1024,
      maxPendingBytes: input.maxPendingBytes ?? 32 * 1024 * 1024,
      maxReadBytes: input.maxReadBytes ?? 32 * 1024 * 1024,
      maxObjects: input.maxObjects ?? 1000,
      maxKeyBytes: input.maxKeyBytes ?? 4096,
      maxMetadataBytes: input.maxMetadataBytes ?? 8192,
      chunkSize: input.chunkSize ?? 64 * 1024,
    },
  };

  return Storage.make({
    provider: "memory",
    storeId,
    capabilities: memoryCapabilities,
    put: (request) => putMemory(context, request),
    get: (request) => getMemory(context, request),
    head: Effect.fnUntraced(function* (request: HeadRequest) {
      const current = yield* Ref.get(state);
      const entry = yield* Effect.fromResult(findObject(current, request, "head"));
      return cloneMetadata(entry.metadata);
    }),
    delete: (request) => deleteMemory(context, request),
    listPage: (request) => listMemory(context, request),
  });
});

export const layer = (options: MemoryOptions = {}) =>
  Layer.effectContext(
    Effect.map(make(options), (storage) =>
      Context.make(Storage, storage).pipe(
        Context.add(TransferDriver, {
          storage,
          maximumObjectBytes: options.maxObjectBytes ?? 16 * 1024 * 1024,
          maximumSingleUploadBytes: options.maxObjectBytes ?? 16 * 1024 * 1024,
          transportOverheadBytes: 0,
        }),
      ),
    ),
  );
