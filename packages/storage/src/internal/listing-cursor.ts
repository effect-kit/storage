import { createHash } from "node:crypto";

import { Effect, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import { storageError } from "../errors.js";

const Cursor = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Literal(1),
    storeId: Schema.String,
    query: Schema.String,
    token: Schema.String,
  }),
);

export const decodeCursor = Effect.fnUntraced(function* (
  provider: string,
  cursor: string | undefined,
  storeId: string,
  query: string,
  operation: string,
) {
  if (cursor === undefined) return undefined;
  if (cursor.length > 65536)
    return yield* storageError(operation, provider, { _tag: "InvalidInput", field: "cursor" });
  const decoded = yield* Effect.fromResult(Base64Url.decodeString(cursor)).pipe(
    Effect.mapError(() =>
      storageError(operation, provider, { _tag: "InvalidInput", field: "cursor" }),
    ),
  );
  const value = yield* Schema.decodeUnknownEffect(Cursor, { onExcessProperty: "error" })(
    decoded,
  ).pipe(
    Effect.mapError(() =>
      storageError(operation, provider, { _tag: "InvalidInput", field: "cursor" }),
    ),
  );
  if (value.storeId !== storeId || value.query !== createHash("sha256").update(query).digest("hex"))
    return yield* storageError(operation, provider, { _tag: "InvalidInput", field: "cursor" });
  return value.token;
});

export function encodeCursor(storeId: string, query: string, token: string) {
  return Base64Url.encode(
    Schema.encodeSync(Cursor)({
      version: 1,
      storeId,
      query: createHash("sha256").update(query).digest("hex"),
      token,
    }),
  );
}
