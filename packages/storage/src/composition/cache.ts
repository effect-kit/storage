import { Cache, Data, Duration, Effect, Exit, Semaphore, Stream } from "effect";

import type { StorageError } from "../core/errors.js";
import { storageError } from "../core/errors.js";
import type { ObjectMetadata } from "../core/object.js";
import type { GetRequest, HeadRequest, ObjectDownload } from "../core/requests.js";
import { Storage } from "../core/storage.js";
import { storageLayer } from "./layer.js";

export interface StorageCacheOptions {
  readonly capacity: number;
  readonly timeToLive: Duration.Input;
  readonly objects?: { readonly capacity: number; readonly maxObjectBytes: number };
}
class MetadataKey extends Data.Class<{ readonly epoch: number; readonly target: string }> {}
class ObjectKey extends Data.Class<{ readonly epoch: number; readonly target: string }> {}
interface CachedObject {
  readonly metadata: ObjectMetadata;
  readonly bytes: Uint8Array;
}

const clone = (value: ObjectMetadata): ObjectMetadata => ({
  ...value,
  ref: { ...value.ref, ...(value.ref.version ? { version: { ...value.ref.version } } : {}) },
  ...(value.customMetadata ? { customMetadata: { ...value.customMetadata } } : {}),
  ...(value.checksums ? { checksums: value.checksums.map((checksum) => ({ ...checksum })) } : {}),
});

export const StorageCache = {
  make: Effect.fnUntraced(function* (options: StorageCacheOptions) {
    if (
      !Number.isSafeInteger(options.capacity) ||
      options.capacity < 1 ||
      (options.objects &&
        (!Number.isSafeInteger(options.objects.capacity) ||
          options.objects.capacity < 1 ||
          !Number.isSafeInteger(options.objects.maxObjectBytes) ||
          options.objects.maxObjectBytes < 1))
    )
      return yield* storageError("cache", "composition", {
        _tag: "InvalidConfiguration",
        field: "capacity",
      });
    const ttl = yield* Effect.try({
      try: () => Duration.fromInputUnsafe(options.timeToLive),
      catch: () =>
        storageError("cache", "composition", { _tag: "InvalidConfiguration", field: "timeToLive" }),
    });
    const source = yield* Storage;
    let epoch = 0;
    // An epoch prevents a lookup racing a mutation from repopulating the current cache.
    const metadata = yield* Cache.makeWith(
      (key: MetadataKey) => source.head(JSON.parse(key.target) as HeadRequest),
      {
        capacity: options.capacity,
        timeToLive: (exit) => (Exit.isSuccess(exit) ? ttl : 0),
      },
    );
    const objects = options.objects;
    const fills = yield* Semaphore.make(1);
    const bytes = objects
      ? yield* Cache.makeWith(
          (key: ObjectKey): Effect.Effect<CachedObject, StorageError> =>
            fills.withPermit(
              Effect.scoped(
                Effect.gen(function* () {
                  const result = yield* source.get(JSON.parse(key.target) as GetRequest);
                  if (
                    result._tag !== "Object" ||
                    result.metadata.size === undefined ||
                    result.metadata.size > objects.maxObjectBytes
                  )
                    return yield* storageError("cache", source.provider, {
                      _tag: "Unsupported",
                      capability: "cache-fill",
                    });
                  let size = 0;
                  const joined = new Uint8Array(result.metadata.size);
                  yield* Stream.runForEach(result.body, (chunk) =>
                    Effect.suspend(() => {
                      if (chunk.length > joined.length - size)
                        return Effect.fail(
                          storageError("cache", source.provider, {
                            _tag: "LimitExceeded",
                            limit: joined.length,
                          }),
                        );
                      joined.set(chunk, size);
                      size += chunk.length;
                      return Effect.void;
                    }),
                  );
                  if (size !== joined.length)
                    return yield* storageError("cache", source.provider, {
                      _tag: "LengthMismatch",
                      expected: joined.length,
                      actual: size,
                    });
                  return { metadata: result.metadata, bytes: joined };
                }),
              ),
            ),
          { capacity: objects.capacity, timeToLive: (exit) => (Exit.isSuccess(exit) ? ttl : 0) },
        )
      : undefined;
    const invalidate = Effect.suspend(() => {
      epoch++;
      return Cache.invalidateAll(metadata).pipe(
        Effect.andThen(bytes ? Cache.invalidateAll(bytes) : Effect.void),
      );
    });
    const rejectDenied = (error: StorageError) =>
      error.reason._tag === "AccessDenied" ? invalidate : Effect.void;
    const getNative = (request: GetRequest) =>
      source.get(request).pipe(Effect.tapError(rejectDenied));
    return Storage.make({
      ...source,
      head: (target) =>
        Cache.get(metadata, new MetadataKey({ epoch, target: JSON.stringify(target) })).pipe(
          Effect.map((value) => clone(value)),
        ),
      // Invalidate even uncertain mutations; a failed response does not prove no write occurred.
      put: (request) => source.put(request).pipe(Effect.ensuring(invalidate)),
      delete: (request) => source.delete(request).pipe(Effect.ensuring(invalidate)),
      get: Effect.fnUntraced(function* (request) {
        if (!bytes || !objects || request.range || request.condition)
          return yield* getNative(request);
        const observed = yield* source.head(request).pipe(Effect.tapError(rejectDenied));
        if (observed.size === undefined || observed.size > objects.maxObjectBytes)
          return yield* getNative(request);
        if (!observed.ref.version && !observed.etag) return yield* getNative(request);
        const pinned: GetRequest = observed.ref.version
          ? { ref: observed.ref }
          : { ref: observed.ref, condition: { _tag: "IfMatch", etag: observed.etag ?? "" } };
        const cached = yield* Cache.get(
          bytes,
          new ObjectKey({ epoch, target: JSON.stringify(pinned) }),
        );
        return {
          _tag: "Object",
          metadata: clone(cached.metadata),
          contentLength: cached.bytes.length,
          body: Stream.suspend(() =>
            Stream.fromIterable(
              (function* () {
                for (let offset = 0; offset < cached.bytes.length; offset += 65536)
                  yield cached.bytes.slice(offset, offset + 65536);
              })(),
              { chunkSize: 1 },
            ),
          ),
        } satisfies ObjectDownload;
      }),
    });
  }),
  layer(options: StorageCacheOptions) {
    return storageLayer(StorageCache.make(options));
  },
};
