import { Effect, Redacted, Schema } from "effect";

import type { MultipartHandle } from "../contracts/multipart.js";
import { storageError } from "../errors.js";
import type { S3StorageService } from "../s3.js";
import type { TransferEndpoint } from "../transfers/models.js";
import { operationMetadataKey } from "../transfers/upload.js";
import { Upload } from "./native-models.js";
import { nativeLimits } from "./native-models.js";

const connections = new WeakMap<TransferEndpoint, S3StorageService>();

/** SDK attempts are already fixed to one by S3Storage; Transfers owns all retry policy. */
export function makeS3TransferDriver(storage: S3StorageService): TransferEndpoint {
  const profile = storage.profile;
  const limits = profile?.limits ?? nativeLimits;
  const endpoint: TransferEndpoint = {
    storage,
    maximumObjectBytes:
      profile?.maximumObjectBytes ?? nativeLimits.maximumPartBytes * nativeLimits.maximumParts,
    maximumSingleUploadBytes: limits.maximumSinglePutBytes,
    transportOverheadBytes: 1024 * 1024,
    multipart: storage.multipart,
    handles: profile ? profileSessionHandles(profile.configuration) : s3SessionHandles,
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
      maximumBytes: limits.maximumSingleCopyBytes,
      accepts: (source) =>
        profile?.copy !== "none" &&
        (profile?.copy !== "bucket" ||
          (connections.get(source)?.config.bucket === storage.config.bucket &&
            !storage.config.encryption &&
            !connections.get(source)?.config.encryption)) &&
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
      ...(profile && !profile.copyPartConditions
        ? {}
        : {
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
          }),
    },
  };
  connections.set(endpoint, storage);
  return endpoint;
}

/** Native checksum policy survives restart alongside the opaque upload identity. */
const s3SessionHandles = {
  encode: (handle: MultipartHandle) =>
    Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(Upload)))(handle).pipe(
      Effect.map(Redacted.make),
      Effect.mapError(() => storageError("handleCodec", "s3", { _tag: "InvalidSession" })),
    ),
  decode: (encoded: Redacted.Redacted<string>) =>
    Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(Upload)))(
      Redacted.value(encoded),
    ).pipe(Effect.mapError(() => storageError("handleCodec", "s3", { _tag: "InvalidSession" }))),
};

function profileSessionHandles(configuration: string) {
  const envelope = Schema.fromJsonString(
    Schema.Struct({ configuration: Schema.Literal(configuration), handle: Schema.String }),
  );
  return {
    encode: (handle: MultipartHandle) =>
      s3SessionHandles.encode(handle).pipe(
        Effect.flatMap((encoded) =>
          Schema.encodeEffect(envelope)({ configuration, handle: Redacted.value(encoded) }),
        ),
        Effect.map(Redacted.make),
        Effect.mapError(() => storageError("handleCodec", "s3", { _tag: "InvalidSession" })),
      ),
    decode: (encoded: Redacted.Redacted<string>) =>
      Schema.decodeUnknownEffect(envelope)(Redacted.value(encoded)).pipe(
        Effect.mapError(() => storageError("handleCodec", "s3", { _tag: "InvalidSession" })),
        Effect.flatMap((value) => s3SessionHandles.decode(Redacted.make(value.handle))),
      ),
  };
}
