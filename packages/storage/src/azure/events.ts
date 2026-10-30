import { Effect, Redacted, Schema } from "effect";

import { storageError } from "../errors.js";
import { StorageEvent } from "../events.js";
import { decodeResponse } from "../internal/provider-response.js";
import { decodeRequest } from "../internal/request-validation.js";

export namespace AzureEvents {
  export const decode = Effect.fnUntraced(function* (
    payload: unknown,
    options: { readonly containerUrl: string; readonly storeId: string },
  ) {
    const records = yield* decodeResponse(
      Schema.Array(
        Schema.Struct({
          id: Schema.NonEmptyString,
          eventType: Schema.NonEmptyString,
          eventTime: Schema.DateTimeUtcFromString,
          data: Schema.Struct({
            url: Schema.String,
            sequencer: Schema.optional(Schema.String),
            versionId: Schema.optional(Schema.String),
          }),
        }),
      ).check(Schema.isMaxLength(1000)),
      payload,
      "decodeEvent",
      "azure",
    );
    const base = URL.parse(options.containerUrl);
    if (!base || base.search || base.hash)
      return yield* storageError("decodeEvent", "azure", {
        _tag: "InvalidConfiguration",
        field: "containerUrl",
      });
    return yield* Effect.forEach(records, (record) =>
      Effect.gen(function* () {
        const url = URL.parse(record.data.url);
        const prefix = `${base.pathname.replace(/\/$/u, "")}/`;
        if (
          !url ||
          url.origin !== base.origin ||
          !url.pathname.startsWith(prefix) ||
          url.username ||
          url.password
        )
          return yield* storageError("decodeEvent", "azure", {
            _tag: "InvalidInput",
            field: "namespace",
          });
        const key = yield* Effect.try({
          try: () => decodeURIComponent(url.pathname.slice(prefix.length)),
          catch: () => storageError("decodeEvent", "azure", { _tag: "InvalidInput", field: "key" }),
        });
        return yield* decodeRequest(
          StorageEvent,
          {
            provider: "azure",
            kind: record.eventType.endsWith("BlobCreated")
              ? "object-created"
              : record.eventType.endsWith("BlobDeleted")
                ? "object-deleted"
                : "other",
            ref: {
              storeId: options.storeId,
              key,
              ...(record.data.versionId
                ? { version: { _tag: "AzureVersion", id: record.data.versionId } }
                : {}),
            },
            occurredAt: record.eventTime,
            id: record.id,
            sequencer: record.data.sequencer,
            nativeType: record.eventType,
            native: Redacted.make(payload),
          },
          "decodeEvent",
          "azure",
        );
      }),
    );
  });
}
