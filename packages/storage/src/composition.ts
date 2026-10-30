import { Effect, Schema } from "effect";

import { storageLayer } from "./composition/layer.js";
import type { StorageError } from "./errors.js";
import { storageError } from "./errors.js";
import type { ObjectMetadata, ObjectRef } from "./object.js";
import type { ListEntry, ObjectTarget } from "./requests.js";
import { digestText } from "./runtime/portable-hash.js";
import { Storage } from "./storage.js";

const PrefixOptions = Schema.Struct({
  prefix: Schema.NonEmptyString.check(Schema.isPattern(/^[^\0]+\/$/u)),
});
const Cursor = Schema.Struct({
  storeId: Schema.String,
  query: Schema.String,
  cursor: Schema.String,
});

export const StoragePrefix = {
  make: Effect.fnUntraced(function* (options: typeof PrefixOptions.Type) {
    const { prefix } = yield* Schema.decodeUnknownEffect(PrefixOptions)(options).pipe(
      Effect.mapError(() =>
        storageError("prefix", "composition", { _tag: "InvalidConfiguration", field: "prefix" }),
      ),
    );
    const source = yield* Storage;
    const storeId = `prefix:${digestText(JSON.stringify([source.storeId, prefix]))}`;
    const invalid = () =>
      storageError("prefix", source.provider, { _tag: "InvalidInput", field: "reference" });
    const refIn = (ref: ObjectRef): Effect.Effect<ObjectRef, StorageError> =>
      ref.storeId !== storeId
        ? Effect.fail(invalid())
        : Effect.succeed({ ...ref, storeId: source.storeId, key: prefix + ref.key });
    const targetIn = (target: ObjectTarget): Effect.Effect<ObjectTarget, StorageError> =>
      "ref" in target
        ? Effect.map(refIn(target.ref), (ref) => ({ ref }))
        : Effect.succeed({ key: prefix + target.key });
    const metadataOut = (metadata: ObjectMetadata): Effect.Effect<ObjectMetadata, StorageError> => {
      if (metadata.ref.storeId !== source.storeId || !metadata.ref.key.startsWith(prefix))
        return Effect.fail(invalid());
      return Effect.succeed({
        ...metadata,
        ref: { ...metadata.ref, storeId, key: metadata.ref.key.slice(prefix.length) },
      });
    };

    return Storage.make({
      provider: source.provider,
      storeId,
      capabilities: source.capabilities,
      put: (request) =>
        source.put({ ...request, key: prefix + request.key }).pipe(Effect.flatMap(metadataOut)),
      head: (request) =>
        targetIn(request).pipe(Effect.flatMap(source.head), Effect.flatMap(metadataOut)),
      get: Effect.fnUntraced(function* (request) {
        const result = yield* source.get({ ...request, ...(yield* targetIn(request)) });
        return { ...result, metadata: yield* metadataOut(result.metadata) };
      }),
      delete: (request) =>
        targetIn(request).pipe(
          Effect.flatMap((target) => source.delete({ ...request, ...target })),
        ),
      listPage: Effect.fnUntraced(function* (request = {}) {
        const query = digestText(
          JSON.stringify([request.prefix ?? "", request.delimiter, request.limit]),
        );
        let cursor: string | undefined;
        if (request.cursor) {
          const decoded = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Cursor))(
            request.cursor,
          ).pipe(Effect.mapError(invalid));
          if (decoded.storeId !== storeId || decoded.query !== query) return yield* invalid();
          cursor = decoded.cursor;
        }
        const page = yield* source.listPage({
          ...request,
          prefix: prefix + (request.prefix ?? ""),
          cursor,
        });
        const entries = yield* Effect.forEach(
          page.entries,
          (entry): Effect.Effect<ListEntry, StorageError> =>
            entry._tag === "Object"
              ? metadataOut(entry.metadata).pipe(
                  Effect.map((metadata) => ({ _tag: "Object" as const, metadata })),
                )
              : entry.prefix.startsWith(prefix)
                ? Effect.succeed({
                    _tag: "CommonPrefix" as const,
                    prefix: entry.prefix.slice(prefix.length),
                  })
                : Effect.fail(invalid()),
        );
        return {
          entries,
          ...(page.cursor
            ? { cursor: JSON.stringify({ storeId, query, cursor: page.cursor }) }
            : {}),
        };
      }),
    });
  }),
  layer(options: typeof PrefixOptions.Type) {
    return storageLayer(StoragePrefix.make(options));
  },
};

const makeReadOnly = Effect.gen(function* () {
  const source = yield* Storage;
  const denied = () =>
    Effect.fail(storageError("readOnly", source.provider, { _tag: "AccessDenied" }, "not-started"));
  return Storage.make({
    ...source,
    capabilities: { ...source.capabilities, putConditions: [], deleteConditions: [] },
    put: denied,
    delete: denied,
  });
});

export const StorageReadOnly = { make: makeReadOnly, layer: storageLayer(makeReadOnly) };
