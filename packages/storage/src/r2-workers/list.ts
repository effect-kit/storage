import { Effect, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import { storageError } from "../errors.js";
import type { ListEntry, ListRequest } from "../requests.js";
import type { R2BucketTransport } from "./binding.js";
import { call, metadata } from "./mapping.js";

const Cursor = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Literal(1),
    storeId: Schema.String,
    prefix: Schema.String,
    delimiter: Schema.NullOr(Schema.String),
    limit: Schema.Natural,
    token: Schema.NonEmptyString,
  }),
);
const invalid = () =>
  storageError("listPage", "r2-workers", { _tag: "InvalidInput", field: "cursor" });

export const list = Effect.fnUntraced(function* (
  bucket: R2BucketTransport,
  storeId: string,
  request: ListRequest = {},
) {
  const { prefix = "", delimiter, limit = 100 } = request;
  let token: string | undefined;
  if (request.cursor) {
    if (request.cursor.length > 32768) return yield* invalid();
    const text = yield* Effect.fromResult(Base64Url.decodeString(request.cursor)).pipe(
      Effect.mapError(invalid),
    );
    const cursor = yield* Schema.decodeUnknownEffect(Cursor)(text).pipe(Effect.mapError(invalid));
    if (
      cursor.storeId !== storeId ||
      cursor.prefix !== prefix ||
      cursor.delimiter !== (delimiter ?? null) ||
      cursor.limit !== limit
    )
      return yield* invalid();
    token = cursor.token;
  }
  const page = yield* call("listPage", () =>
    bucket.list({
      prefix,
      ...(delimiter === undefined ? {} : { delimiter }),
      limit,
      ...(token === undefined ? {} : { cursor: token }),
      include: ["httpMetadata", "customMetadata"],
    }),
  );
  if (page.truncated && !page.cursor)
    return yield* storageError("listPage", "r2-workers", {
      _tag: "ProviderFailure",
      code: "MissingCursor",
    });
  const objects = yield* Effect.forEach(page.objects, (object) =>
    metadata(storeId, object).pipe(
      Effect.map((saved): ListEntry => ({ _tag: "Object", metadata: saved })),
    ),
  );
  const prefixes: ListEntry[] = page.delimitedPrefixes.map((common) => ({
    _tag: "CommonPrefix",
    prefix: common,
  }));
  return {
    entries: [...objects, ...prefixes],
    ...(page.truncated && page.cursor
      ? {
          cursor: Base64Url.encode(
            Schema.encodeSync(Cursor)({
              version: 1,
              storeId,
              prefix,
              delimiter: delimiter ?? null,
              limit,
              token: page.cursor,
            }),
          ),
        }
      : {}),
  };
});
