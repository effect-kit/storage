import { Effect, Redacted, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import { decodeResponse } from "../../internal/provider-response.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { StorageEvent } from "../../lifecycle/events.js";

const Event = Schema.Struct({
  eventVersion: Schema.String,
  eventTime: Schema.DateTimeUtcFromString,
  eventName: Schema.NonEmptyString,
  s3: Schema.Struct({
    bucket: Schema.Struct({ name: Schema.NonEmptyString }),
    object: Schema.Struct({
      key: Schema.String,
      versionId: Schema.optional(Schema.String),
      sequencer: Schema.optional(Schema.String),
    }),
  }),
});
export namespace S3Events {
  export const decode = Effect.fnUntraced(function* (
    payload: unknown,
    options: {
      readonly bucket: string;
      readonly storeId: string;
      readonly provider?: string;
      readonly messageId?: string;
    },
  ) {
    const batch = yield* decodeResponse(
      Schema.Struct({ Records: Schema.Array(Event).check(Schema.isMaxLength(1000)) }),
      payload,
      "decodeEvent",
      "s3",
    );
    return yield* Effect.forEach(batch.Records, (record, index) =>
      Effect.gen(function* () {
        if (record.s3.bucket.name !== options.bucket || !record.eventVersion.startsWith("2."))
          return yield* storageError("decodeEvent", "s3", {
            _tag: "InvalidInput",
            field: "bucket-or-version",
          });
        const key = yield* Effect.try({
          try: () => decodeURIComponent(record.s3.object.key.replace(/\+/gu, " ")),
          catch: () =>
            storageError("decodeEvent", "s3", { _tag: "InvalidInput", field: "encoded-key" }),
        });
        const object = record.s3.object;
        const kind = record.eventName.startsWith("ObjectCreated:")
          ? "object-created"
          : record.eventName === "ObjectRemoved:DeleteMarkerCreated"
            ? "delete-marker-created"
            : record.eventName.startsWith("ObjectRemoved:")
              ? "object-deleted"
              : record.eventName === "ObjectRestore:Completed"
                ? "restore-completed"
                : record.eventName.startsWith("ObjectTagging:")
                  ? "metadata-updated"
                  : "other";
        return yield* decodeRequest(
          StorageEvent,
          {
            provider: options.provider ?? "s3",
            kind,
            ref: {
              storeId: options.storeId,
              key,
              ...(object.versionId ? { version: { _tag: "S3Version", id: object.versionId } } : {}),
            },
            occurredAt: record.eventTime,
            nativeType: record.eventName,
            native: Redacted.make(payload),
            ...(object.sequencer
              ? {
                  sequencer: object.sequencer,
                  id: JSON.stringify([
                    record.s3.bucket.name,
                    key,
                    record.eventName,
                    object.versionId,
                    object.sequencer,
                  ]),
                }
              : options.messageId
                ? { id: `${options.messageId}:${String(index)}` }
                : {}),
          },
          "decodeEvent",
          "s3",
        );
      }),
    );
  });
}
