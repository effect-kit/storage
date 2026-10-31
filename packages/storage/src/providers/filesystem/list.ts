import { Effect, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import type { StorageError } from "../../core/errors.js";
import { storageError } from "../../core/errors.js";
import type { ListEntry, ListPage, ListRequest } from "../../core/requests.js";
import { readEnvelope } from "./envelope.js";
import { mapIO } from "./layout.js";
import type { FilesystemContext } from "./layout.js";

const Cursor = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Literal(1),
    root: Schema.String,
    storeId: Schema.String,
    prefix: Schema.String,
    delimiter: Schema.NullOr(Schema.String),
    limit: Schema.Natural,
    after: Schema.String,
  }),
);

const invalid = () =>
  storageError("listPage", "filesystem", { _tag: "InvalidInput", field: "cursor" });

export const list = Effect.fnUntraced(function* (
  context: FilesystemContext,
  request: ListRequest = {},
): Effect.fn.Return<ListPage, StorageError> {
  const { prefix = "", delimiter, limit = 100 } = request;
  let after: string | undefined;
  if (request.cursor) {
    if (request.cursor.length > 32768) return yield* invalid();
    const text = yield* Effect.fromResult(Base64Url.decodeString(request.cursor)).pipe(
      Effect.mapError(invalid),
    );
    const cursor = yield* Schema.decodeUnknownEffect(Cursor)(text).pipe(Effect.mapError(invalid));
    if (
      cursor.root !== context.rootId ||
      cursor.storeId !== context.storeId ||
      cursor.prefix !== prefix ||
      cursor.delimiter !== (delimiter ?? null) ||
      cursor.limit !== limit
    )
      return yield* invalid();
    after = cursor.after;
  }
  yield* context.checkDirectories;
  // readDirectory allocates an array. This profile deliberately documents O(directory) scans.
  const names = yield* context.fs
    .readDirectory(context.objects)
    .pipe(Effect.mapError(mapIO("listPage")));
  if (names.length > context.maxListEntries)
    return yield* storageError("listPage", "filesystem", {
      _tag: "LimitExceeded",
      limit: context.maxListEntries,
    });
  const entries = new Map<string, ListEntry>();
  for (const name of names) {
    if (!/^[a-f0-9]{64}\.object$/u.test(name)) continue;
    const filename = context.path.join(context.objects, name);
    const metadata = yield* Effect.scoped(
      Effect.gen(function* () {
        const file = yield* context.open(filename);
        const saved = yield* readEnvelope(context, file);
        if ((yield* context.filename(saved.ref.key)) !== filename) return yield* invalid();
        return saved;
      }),
    ).pipe(
      Effect.mapError((error) =>
        error._tag === "PlatformError" ? mapIO("listPage")(error) : error,
      ),
      Effect.catchReason("StorageError", "NotFound", () => Effect.undefined),
    );
    if (!metadata || !metadata.ref.key.startsWith(prefix)) continue;
    const key = metadata.ref.key;
    const index = delimiter ? key.indexOf(delimiter, prefix.length) : -1;
    if (delimiter && index >= 0) {
      const common = key.slice(0, index + delimiter.length);
      entries.set(common, { _tag: "CommonPrefix", prefix: common });
    } else entries.set(key, { _tag: "Object", metadata });
  }
  const sorted = [...entries.keys()].toSorted().filter((key) => after === undefined || key > after);
  const keys = sorted.slice(0, limit);
  const last = keys.at(-1);
  return {
    entries: keys.flatMap((key) => {
      const entry = entries.get(key);
      return entry ? [entry] : [];
    }),
    ...(last !== undefined && sorted.length > limit
      ? {
          cursor: Base64Url.encode(
            Schema.encodeSync(Cursor)({
              version: 1,
              root: context.rootId,
              storeId: context.storeId,
              prefix,
              delimiter: delimiter ?? null,
              limit,
              after: last,
            }),
          ),
        }
      : {}),
  };
});
