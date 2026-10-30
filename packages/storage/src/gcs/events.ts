import { Effect, Redacted, Schema } from "effect";

import { storageError } from "../errors.js";
import { StorageEvent } from "../events.js";
import { decodeResponse } from "../internal/provider-response.js";
import { decodeRequest } from "../internal/request-validation.js";

export namespace GcsEvents {
  /** Accepts the Pub/Sub message after transport authentication and payload decoding. */
  export const decode = Effect.fnUntraced(function* (
    payload: unknown,
    options: { readonly bucket: string; readonly storeId: string },
  ) {
    const value = yield* decodeResponse(
      Schema.Struct({
        messageId: Schema.NonEmptyString,
        publishTime: Schema.DateTimeUtcFromString,
        attributes: Schema.Struct({
          bucketId: Schema.NonEmptyString,
          objectId: Schema.NonEmptyString,
          objectGeneration: Schema.optional(Schema.String.check(Schema.isPattern(/^\d+$/u))),
          eventType: Schema.NonEmptyString,
        }),
      }),
      payload,
      "decodeEvent",
      "gcs",
    );
    if (value.attributes.bucketId !== options.bucket)
      return yield* storageError("decodeEvent", "gcs", { _tag: "InvalidInput", field: "bucket" });
    return [
      yield* decodeRequest(
        StorageEvent,
        {
          provider: "gcs",
          kind:
            value.attributes.eventType === "OBJECT_FINALIZE"
              ? "object-created"
              : value.attributes.eventType === "OBJECT_DELETE"
                ? "object-deleted"
                : value.attributes.eventType === "OBJECT_ARCHIVE"
                  ? "version-archived"
                  : value.attributes.eventType === "OBJECT_METADATA_UPDATE"
                    ? "metadata-updated"
                    : "other",
          ref: {
            storeId: options.storeId,
            key: value.attributes.objectId,
            ...(value.attributes.objectGeneration
              ? { version: { _tag: "GcsGeneration", id: value.attributes.objectGeneration } }
              : {}),
          },
          occurredAt: value.publishTime,
          id: value.messageId,
          nativeType: value.attributes.eventType,
          native: Redacted.make(payload),
        },
        "decodeEvent",
        "gcs",
      ),
    ];
  });
}
