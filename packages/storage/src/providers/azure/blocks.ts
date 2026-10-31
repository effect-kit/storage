import { Effect, Schema } from "effect";

import type { BlockUploads } from "../../core/contracts/resumable.js";
import { storageError } from "../../core/errors.js";
import type { ObjectRef } from "../../core/object.js";
import { uploadBody } from "../../internal/node/upload-body.js";
import { encryption, lease } from "./config.js";
import type { AzureConnection } from "./config.js";
import { condition, metadata, nativeHeaders, target, writeMetadata } from "./mapping.js";
import { request } from "./transport.js";

const BlockId = Schema.NonEmptyString.check(
  Schema.isPattern(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u),
  Schema.isMaxLength(88),
);
const mapBlocks = (blocks: ReadonlyArray<{ name: string; size: number }> | undefined) =>
  (blocks ?? []).map((block) => ({ blockId: block.name, size: block.size }));

export function blockUploads(connection: AzureConnection): BlockUploads {
  const blob = Effect.fnUntraced(function* (ref: ObjectRef) {
    const address = yield* target(connection, { ref }, "blocks");
    if (ref.version)
      return yield* storageError(
        "blocks",
        "azure",
        { _tag: "InvalidInput", field: "ref.version" },
        "not-started",
      );
    return connection.client.getBlockBlobClient(address.ref.key);
  });
  return {
    limits: { maximumBlockBytes: 4000 * 1024 * 1024, maximumBlocks: 50_000 },
    stage: Effect.fnUntraced(function* (input) {
      const client = yield* blob(input.ref);
      yield* Schema.decodeUnknownEffect(BlockId)(input.blockId).pipe(
        Effect.mapError(() =>
          storageError(
            "stageBlock",
            "azure",
            { _tag: "InvalidInput", field: "blockId" },
            "not-started",
          ),
        ),
      );
      if (!input.contentLength || input.contentLength > 4000 * 1024 * 1024)
        return yield* storageError(
          "stageBlock",
          "azure",
          { _tag: "InvalidInput", field: "contentLength" },
          "not-started",
        );
      yield* uploadBody(
        { provider: "azure", maxUnknownLengthBytes: connection.config.maxUnknownLengthBytes },
        input,
        "stageBlock",
        (body, length) =>
          request(connection, "stageBlock", true, (signal) =>
            client.stageBlock(input.blockId, body, length, {
              abortSignal: signal,
              conditions: lease(connection.config),
              ...encryption(connection.config),
              contentChecksumAlgorithm: connection.config.checksumAlgorithm,
            }),
          ),
      );
    }),
    list: Effect.fnUntraced(function* (ref) {
      const client = yield* blob(ref);
      const result = yield* request(connection, "listBlocks", false, (signal) =>
        client.getBlockList("all", { abortSignal: signal, conditions: lease(connection.config) }),
      ).pipe(
        Effect.catchReason("StorageError", "NotFound", () =>
          Effect.succeed({ committedBlocks: [], uncommittedBlocks: [] }),
        ),
      );
      return {
        committed: mapBlocks(result.committedBlocks),
        uncommitted: mapBlocks(result.uncommittedBlocks),
      };
    }),
    commit: Effect.fnUntraced(function* (input) {
      const client = yield* blob(input.ref);
      if (
        input.blockIds.length > 50_000 ||
        new Set(input.blockIds).size !== input.blockIds.length ||
        new Set(input.blockIds.map((id) => Buffer.from(id, "base64").length)).size > 1
      )
        return yield* storageError(
          "commitBlocks",
          "azure",
          { _tag: "InvalidInput", field: "blockIds" },
          "not-started",
        );
      yield* Effect.forEach(input.blockIds, (id) =>
        Schema.decodeUnknownEffect(BlockId)(id).pipe(
          Effect.mapError(() =>
            storageError(
              "commitBlocks",
              "azure",
              { _tag: "InvalidInput", field: "blockId" },
              "not-started",
            ),
          ),
        ),
      );
      const headers = input.headers ?? {};
      const customMetadata = yield* writeMetadata(headers);
      const result = yield* request(connection, "commitBlocks", true, (signal) =>
        client.commitBlockList([...input.blockIds], {
          abortSignal: signal,
          conditions: { ...condition(input.condition), ...lease(connection.config) },
          blobHTTPHeaders: nativeHeaders(headers),
          metadata: customMetadata,
          ...encryption(connection.config),
        }),
      );
      return metadata(connection, input.ref.key, {
        ...result,
        ...headers,
        metadata: customMetadata,
      });
    }),
  };
}
