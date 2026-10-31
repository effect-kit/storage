import { Effect, Redacted, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import { decodeResponse } from "../../internal/provider-response.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { StorageEvent } from "../../lifecycle/events.js";

export namespace TigrisEvents {
  /** messageId must identify an authenticated delivery; native payloads have no stable event ID. */
  export const decode = Effect.fnUntraced(function* (
    payload: unknown,
    options: { readonly bucket: string; readonly storeId: string; readonly messageId: string },
  ) {
    yield* decodeRequest(Schema.NonEmptyString, options.messageId, "decodeEvent", "tigris");
    const batch = yield* decodeResponse(
      Schema.Struct({
        events: Schema.Array(
          Schema.Struct({
            eventVersion: Schema.NonEmptyString,
            eventSource: Schema.Literal("tigris"),
            eventName: Schema.NonEmptyString,
            eventTime: Schema.DateTimeUtcFromString,
            bucket: Schema.NonEmptyString,
            object: Schema.Struct({ key: Schema.NonEmptyString }),
          }),
        ).check(Schema.isMaxLength(1000)),
      }),
      payload,
      "decodeEvent",
      "tigris",
    );
    return yield* Effect.forEach(batch.events, (event, index) =>
      Effect.gen(function* () {
        if (event.bucket !== options.bucket)
          return yield* storageError("decodeEvent", "tigris", {
            _tag: "InvalidInput",
            field: "bucket",
          });
        return yield* decodeRequest(
          StorageEvent,
          {
            provider: "tigris",
            kind:
              event.eventName === "OBJECT_CREATED_PUT"
                ? "object-created"
                : event.eventName === "OBJECT_DELETED"
                  ? "object-deleted"
                  : "other",
            ref: { storeId: options.storeId, key: event.object.key },
            occurredAt: event.eventTime,
            id: `${options.messageId}:${String(index)}`,
            nativeType: event.eventName,
            native: Redacted.make(payload),
          },
          "decodeEvent",
          "tigris",
        );
      }),
    );
  });
}
