import { Effect, Redacted, Schema } from "effect";

import { storageError } from "../errors.js";
import { StorageEvent } from "../events.js";
import { decodeResponse } from "../internal/provider-response.js";
import { decodeRequest } from "../internal/request-validation.js";

export namespace R2Events {
  export const decode = Effect.fnUntraced(function* (
    payload: unknown,
    options: { readonly bucket: string; readonly storeId: string; readonly messageId: string },
  ) {
    const value = yield* decodeResponse(
      Schema.Struct({
        bucket: Schema.NonEmptyString,
        action: Schema.NonEmptyString,
        eventTime: Schema.DateTimeUtcFromString,
        object: Schema.Struct({ key: Schema.NonEmptyString }),
      }),
      payload,
      "decodeEvent",
      "r2",
    );
    if (value.bucket !== options.bucket)
      return yield* storageError("decodeEvent", "r2", { _tag: "InvalidInput", field: "bucket" });
    return [
      yield* decodeRequest(
        StorageEvent,
        {
          provider: "r2",
          kind: value.action.includes("Delete")
            ? "object-deleted"
            : value.action.includes("Put") ||
                value.action.includes("Complete") ||
                value.action.includes("Copy")
              ? "object-created"
              : "other",
          ref: { storeId: options.storeId, key: value.object.key },
          occurredAt: value.eventTime,
          id: options.messageId,
          nativeType: value.action,
          native: Redacted.make(payload),
        },
        "decodeEvent",
        "r2",
      ),
    ];
  });
}
