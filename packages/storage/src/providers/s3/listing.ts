import { Effect } from "effect";

import { ListObjectsV2Command } from "@aws-sdk/client-s3";

import { storageError } from "../../core/errors.js";
import type { ListEntry, ListRequest } from "../../core/requests.js";
import { decodeCursor, encodeCursor } from "../../internal/listing-cursor.js";
import { authority } from "./config.js";
import type { S3Connection } from "./config.js";
import { metadata } from "./object-mapping.js";
import { request } from "./transport.js";

export const listPage = Effect.fnUntraced(function* (
  connection: S3Connection,
  input: ListRequest = {},
) {
  const { config, client } = connection;
  const query = JSON.stringify([
    "objects",
    ...(connection.profile ? [connection.profile.provider, connection.profile.configuration] : []),
    input.prefix ?? "",
    input.delimiter ?? null,
    input.limit ?? 1000,
  ]);
  const token = yield* decodeCursor("s3", input.cursor, config.storeId, query, "listPage");
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
