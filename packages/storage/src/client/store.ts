import { Effect, Schema } from "effect";
import type { KeyValueStore } from "effect/persistence";

import { UploadId, UploadInput, failUpload } from "../uploads/schema.js";
export const UploadTicket = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.optional(UploadId),
  idempotencyKey: Schema.NonEmptyString,
  input: UploadInput,
});
export type UploadTicket = typeof UploadTicket.Type;
export interface ClientUploadStore {
  readonly read: (
    id: string,
  ) => Effect.Effect<UploadTicket | undefined, ReturnType<typeof failUpload>>;
  readonly write: (
    id: string,
    ticket: UploadTicket,
  ) => Effect.Effect<void, ReturnType<typeof failUpload>>;
  readonly remove: (id: string) => Effect.Effect<void, ReturnType<typeof failUpload>>;
}
export const UploadClientStore = {
  memory(): ClientUploadStore {
    const records = new Map<string, UploadTicket>();
    return {
      read: (id) => Effect.sync(() => records.get(id)),
      write: (id, ticket) =>
        Effect.sync(() => {
          records.set(id, structuredClone(ticket));
        }),
      remove: (id) =>
        Effect.sync(() => {
          records.delete(id);
        }),
    };
  },
  keyValueStore(kvs: KeyValueStore.KeyValueStore, namespace: string): ClientUploadStore {
    const key = (id: string) => `${namespace}:browser-upload:${encodeURIComponent(id)}`;
    return {
      read: (id) =>
        kvs.get(key(id)).pipe(
          Effect.mapError(() => failUpload("Unavailable")),
          Effect.flatMap((raw) =>
            raw === undefined
              ? Effect.succeed(undefined)
              : raw.length > 65536
                ? Effect.fail(failUpload("BadRequest"))
                : Schema.decodeUnknownEffect(Schema.fromJsonString(UploadTicket))(raw).pipe(
                    Effect.mapError(() => failUpload("BadRequest")),
                  ),
          ),
        ),
      write: (id, ticket) =>
        Schema.decodeUnknownEffect(UploadTicket, { onExcessProperty: "error" })(ticket).pipe(
          Effect.mapError(() => failUpload("BadRequest")),
          Effect.flatMap((value) =>
            kvs
              .set(key(id), JSON.stringify(value))
              .pipe(Effect.mapError(() => failUpload("Unavailable"))),
          ),
        ),
      remove: (id) => kvs.remove(key(id)).pipe(Effect.mapError(() => failUpload("Unavailable"))),
    };
  },
};
