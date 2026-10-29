import { Effect, Redacted, Schema } from "effect";

import type { MultipartHandle } from "../contracts/multipart.js";
import type { S3StorageService } from "../s3.js";
import { sessionError } from "../sessions/store.js";
import type { TransferEndpoint } from "../transfers/models.js";
import { operationMetadataKey } from "../transfers/upload.js";
import { Upload } from "./native-models.js";
import { nativeLimits } from "./native-models.js";

const connections = new WeakMap<TransferEndpoint, S3StorageService>();

/** SDK attempts are already fixed to one by S3Storage; Transfers owns all retry policy. */
export function s3TransferEndpoint(storage: S3StorageService): TransferEndpoint {
  const endpoint: TransferEndpoint = {
    storage,
    maximumObjectBytes: nativeLimits.maximumPartBytes * nativeLimits.maximumParts,
    maximumSingleUploadBytes: nativeLimits.maximumSinglePutBytes,
    transportOverheadBytes: 1024 * 1024,
    multipart: storage.multipart,
    reconcile: ({ key, operationId, size }) =>
      storage.head({ key }).pipe(
        Effect.map((metadata) =>
          metadata.customMetadata?.[operationMetadataKey] === operationId && metadata.size === size
            ? metadata
            : undefined,
        ),
        Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(undefined)),
      ),
    nativeCopy: {
      maximumBytes: nativeLimits.maximumSingleCopyBytes,
      accepts: (source) =>
        connections.get(source)?.config.endpoint === storage.config.endpoint &&
        connections.has(source),
      copy: (intent) =>
        storage.copyObject(
          {
            source: intent.ref,
            destinationKey: intent.destinationKey,
            sourceIfMatch: intent.etag,
            condition: intent.condition,
            headers: intent.headers,
            metadataDirective: "REPLACE",
          },
          connections.get(intent.source),
        ),
      part: (intent) =>
        storage
          .copyPart(
            {
              upload: intent.upload,
              partNumber: intent.partNumber,
              source: intent.ref,
              sourceIfMatch: intent.etag,
              range: { _tag: "Bounded", start: intent.start, end: intent.end },
            },
            connections.get(intent.source),
          )
          .pipe(
            Effect.map((part) => ({
              partNumber: part.partNumber,
              etag: part.etag,
              checksums: part.checksums,
              size: intent.end - intent.start + 1,
            })),
          ),
    },
  };
  connections.set(endpoint, storage);
  return endpoint;
}

/** Native checksum policy survives restart alongside the opaque upload identity. */
export const s3SessionHandles = {
  encode: (handle: MultipartHandle) =>
    Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(Upload)))(handle).pipe(
      Effect.map(Redacted.make),
      Effect.mapError(() => sessionError("InvalidS3Handle")),
    ),
  decode: (encoded: Redacted.Redacted<string>) =>
    Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(Upload)))(
      Redacted.value(encoded),
    ).pipe(Effect.mapError(() => sessionError("InvalidS3Handle"))),
};
