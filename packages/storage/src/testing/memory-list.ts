import { Effect, Ref, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import type { ListEntry, ListPage, ListRequest } from "../requests.js";
import { cloneMetadata } from "./memory-state.js";
import type { MemoryContext } from "./memory-state.js";

const Cursor = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Literal(1),
    storeId: Schema.String,
    instanceId: Schema.String,
    prefix: Schema.String,
    delimiter: Schema.NullOr(Schema.String),
    limit: Schema.Natural,
    after: Schema.String,
  }),
);

export const listMemory = Effect.fnUntraced(function* (
  context: MemoryContext,
  request: ListRequest = {},
): Effect.fn.Return<ListPage, StorageError> {
  const prefix = request.prefix ?? "";
  const delimiter = request.delimiter ?? null;
  const limit = request.limit ?? 100;
  let after: string | undefined;

  if (request.cursor !== undefined) {
    if (request.cursor.length > 32768) {
      return yield* storageError("listPage", "memory", { _tag: "InvalidInput", field: "cursor" });
    }

    const decoded = yield* Effect.fromResult(Base64Url.decodeString(request.cursor)).pipe(
      Effect.mapError(() =>
        storageError("listPage", "memory", { _tag: "InvalidInput", field: "cursor" }),
      ),
    );
    const cursor = yield* Schema.decodeUnknownEffect(Cursor)(decoded).pipe(
      Effect.mapError(() =>
        storageError("listPage", "memory", { _tag: "InvalidInput", field: "cursor" }),
      ),
    );

    if (
      cursor.storeId !== context.storeId ||
      cursor.instanceId !== context.instanceId ||
      cursor.prefix !== prefix ||
      cursor.delimiter !== delimiter ||
      cursor.limit !== limit
    ) {
      return yield* storageError("listPage", "memory", { _tag: "InvalidInput", field: "cursor" });
    }
    after = cursor.after;
  }

  const state = yield* Ref.get(context.state);
  const entries = new Map<string, ListEntry>();
  for (const [key, entry] of state.objects) {
    if (!key.startsWith(prefix)) continue;
    const index = delimiter === null ? -1 : key.indexOf(delimiter, prefix.length);
    if (index >= 0 && delimiter !== null) {
      const common = key.slice(0, index + delimiter.length);
      entries.set(common, { _tag: "CommonPrefix", prefix: common });
    } else {
      entries.set(key, { _tag: "Object", metadata: cloneMetadata(entry.metadata) });
    }
  }

  const sorted = Array.from(entries.keys())
    .toSorted()
    .filter((key) => after === undefined || key > after);
  const keys = sorted.slice(0, limit);
  const page: ListEntry[] = [];
  for (const key of keys) {
    const entry = entries.get(key);
    if (entry !== undefined) page.push(entry);
  }

  const last = keys.at(-1);
  return {
    entries: page,
    ...(sorted.length > limit && last !== undefined
      ? {
          cursor: Base64Url.encode(
            Schema.encodeSync(Cursor)({
              version: 1,
              storeId: context.storeId,
              instanceId: context.instanceId,
              prefix,
              delimiter,
              limit,
              after: last,
            }),
          ),
        }
      : {}),
  };
});
