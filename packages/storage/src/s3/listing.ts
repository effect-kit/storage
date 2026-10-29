import { createHash } from "node:crypto";

import { Effect, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import { ListObjectsV2Command } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import type { ListEntry, ListRequest } from "../requests.js";
import { authority } from "./config.js";
import type { S3Connection } from "./config.js";
import { metadata } from "./object-mapping.js";
import { request } from "./transport.js";

const Cursor = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Literal(1),
    storeId: Schema.String,
    query: Schema.String,
    token: Schema.String,
  }),
);

export const decodeCursor = Effect.fnUntraced(function* (
  cursor: string | undefined,
  storeId: string,
  query: string,
  operation: string,
) {
  if (cursor === undefined) return undefined;
  if (cursor.length > 65536)
    return yield* storageError(operation, "s3", { _tag: "InvalidInput", field: "cursor" });
  const decoded = yield* Effect.fromResult(Base64Url.decodeString(cursor)).pipe(
    Effect.mapError(() => storageError(operation, "s3", { _tag: "InvalidInput", field: "cursor" })),
  );
  const value = yield* Schema.decodeUnknownEffect(Cursor, { onExcessProperty: "error" })(
    decoded,
  ).pipe(
    Effect.mapError(() => storageError(operation, "s3", { _tag: "InvalidInput", field: "cursor" })),
  );
  if (value.storeId !== storeId || value.query !== createHash("sha256").update(query).digest("hex"))
    return yield* storageError(operation, "s3", { _tag: "InvalidInput", field: "cursor" });
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

export const listPage = Effect.fnUntraced(function* (
  connection: S3Connection,
  input: ListRequest = {},
) {
  const { config, client } = connection;
  const query = JSON.stringify([
    "objects",
    input.prefix ?? "",
    input.delimiter ?? null,
    input.limit ?? 1000,
  ]);
  const token = yield* decodeCursor(input.cursor, config.storeId, query, "listPage");
  const output = yield* request(connection, "listPage", false, (signal) =>
    client.send(
      new ListObjectsV2Command({
        ...authority(config),
        Prefix: input.prefix,
        Delimiter: input.delimiter,
        MaxKeys: input.limit ?? 1000,
        ContinuationToken: token,
      }),
      { abortSignal: signal },
    ),
  );

  const entries: ListEntry[] = [];
  for (const object of output.Contents ?? []) {
    if (object.Key === undefined)
      return yield* storageError("listPage", "s3", {
        _tag: "ProviderFailure",
        code: "MissingObjectKey",
      });
    entries.push({
      _tag: "Object",
      metadata: metadata(config, object.Key, { ...object, ContentLength: object.Size }),
    });
  }
  for (const prefix of output.CommonPrefixes ?? []) {
    if (prefix.Prefix !== undefined) entries.push({ _tag: "CommonPrefix", prefix: prefix.Prefix });
  }
  if (output.IsTruncated && !output.NextContinuationToken)
    return yield* storageError("listPage", "s3", {
      _tag: "ProviderFailure",
      code: "MissingContinuation",
    });
  return {
    entries,
    ...(output.IsTruncated && output.NextContinuationToken
      ? { cursor: encodeCursor(config.storeId, query, output.NextContinuationToken) }
      : {}),
  };
});
