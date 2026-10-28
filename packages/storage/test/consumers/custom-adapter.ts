import { Effect, Layer, Random, Schema, Stream } from "effect";

import { Storage, StorageError, StoragePublicUrls } from "@effect-kit/storage";
import type { ObjectMetadata, ObjectTarget, PutRequest, StorageService } from "@effect-kit/storage";

const Cursor = Schema.fromJsonString(
  Schema.Struct({
    storeId: Schema.String,
    prefix: Schema.String,
    limit: Schema.Natural,
    after: Schema.String,
  }),
);

const failure = (operation: string, reason: StorageError["reason"]) =>
  new StorageError({
    operation,
    provider: "custom",
    reason,
    mutationOutcome: operation === "put" || operation === "delete" ? "rejected" : "not-applicable",
  });
const keyOf = (target: ObjectTarget) => ("ref" in target ? target.ref.key : target.key);

/** An intentionally small independent adapter, using only public exports. */
export const makeCustomAdapter = Effect.fnUntraced(function* () {
  const storeId = `custom:${String(yield* Random.nextInt)}`;
  const objects = new Map<string, { metadata: ObjectMetadata; bytes: Uint8Array }>();
  let revision = 0;
  const lookup = (target: ObjectTarget, operation: string) =>
    Effect.suspend(() => {
      const found = objects.get(keyOf(target));
      return found ? Effect.succeed(found) : Effect.fail(failure(operation, { _tag: "NotFound" }));
    });
  return Storage.make({
    provider: "custom",
    storeId,
    capabilities: {
      putConditions: [],
      getConditions: [],
      deleteConditions: [],
      ranges: [],
      versionedReads: "none",
      list: { maxPageSize: 1, delimiter: false, cursorPersistence: "process", snapshot: false },
    },
    put: Effect.fnUntraced(function* <E, R>(request: PutRequest<E, R>) {
      const source =
        request.body instanceof Uint8Array ? Stream.succeed(request.body) : request.body;
      const chunks: Uint8Array[] = [];
      let length = 0;
      yield* Stream.runForEach(source, (chunk) =>
        Effect.suspend(() => {
          if (length + chunk.length > 1024)
            return Effect.fail(failure("put", { _tag: "LimitExceeded", limit: 1024 }));
          length += chunk.length;
          chunks.push(Uint8Array.from(chunk));
          return Effect.void;
        }),
      );
      const bytes = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.length;
      }
      revision++;
      const metadata: ObjectMetadata = {
        ref: { storeId, key: request.key },
        size: length,
        etag: `custom-${String(revision)}`,
        contentType: request.contentType,
        customMetadata: request.customMetadata ? { ...request.customMetadata } : undefined,
      };
      objects.set(request.key, { metadata, bytes });
      return { ...metadata };
    }),
    head: (target) => lookup(target, "head").pipe(Effect.map((found) => ({ ...found.metadata }))),
    get: (target) =>
      lookup(target, "get").pipe(
        Effect.map((found) => ({
          _tag: "Object" as const,
          metadata: { ...found.metadata },
          contentLength: found.bytes.length,
          body: Stream.succeed(Uint8Array.from(found.bytes)),
        })),
      ),
    delete: (target) =>
      Effect.sync(() => {
        objects.delete(keyOf(target));
      }),
    listPage: Effect.fnUntraced(function* (request = {}) {
      const prefix = request.prefix ?? "";
      const limit = request.limit ?? 1;
      let after: string | undefined;
      if (request.cursor) {
        const cursor = yield* Schema.decodeUnknownEffect(Cursor)(request.cursor).pipe(
          Effect.mapError(() => failure("listPage", { _tag: "InvalidInput", field: "cursor" })),
        );
        if (cursor.storeId !== storeId || cursor.prefix !== prefix || cursor.limit !== limit) {
          return yield* failure("listPage", { _tag: "InvalidInput", field: "cursor" });
        }
        after = cursor.after;
      }
      const keys = [...objects.keys()]
        .filter((key) => key.startsWith(prefix) && (after === undefined || key > after))
        .toSorted();
      const key = keys.at(0);
      const found = key === undefined ? undefined : objects.get(key);
      return {
        entries: found ? [{ _tag: "Object" as const, metadata: { ...found.metadata } }] : [],
        ...(keys.length > 1 && key !== undefined
          ? {
              cursor: Schema.encodeSync(Cursor)({ storeId, prefix, limit, after: key }),
            }
          : {}),
      };
    }),
  } satisfies StorageService);
});

export function publicUrls(storage: StorageService) {
  return Layer.succeed(
    StoragePublicUrls,
    StoragePublicUrls.of({
      url: (ref) =>
        ref.storeId === storage.storeId
          ? Effect.succeed(`https://example.invalid/${encodeURIComponent(ref.key)}`)
          : Effect.fail(
              new StorageError({
                operation: "url",
                provider: "custom",
                reason: { _tag: "InvalidInput", field: "ref.storeId" },
                mutationOutcome: "not-applicable",
              }),
            ),
    }),
  );
}
