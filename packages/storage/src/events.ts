import { Context, Effect, Layer, Schema, Semaphore } from "effect";
import { KeyValueStore } from "effect/persistence";

import { storageError } from "./errors.js";
import type { StorageError } from "./errors.js";
import { ObjectRef } from "./object.js";

export const StorageEvent = Schema.Struct({
  provider: Schema.NonEmptyString,
  kind: Schema.Literals([
    "object-created",
    "object-deleted",
    "delete-marker-created",
    "version-archived",
    "metadata-updated",
    "restore-completed",
    "other",
  ]),
  ref: ObjectRef,
  occurredAt: Schema.DateTimeUtc,
  id: Schema.optional(Schema.NonEmptyString),
  sequencer: Schema.optional(Schema.String.check(Schema.isPattern(/^[a-fA-F0-9]{1,128}$/u))),
  nativeType: Schema.NonEmptyString,
  native: Schema.optional(Schema.Redacted(Schema.Unknown)),
});
export type StorageEvent = typeof StorageEvent.Type;
const Record = Schema.Struct({ id: Schema.String, sequencer: StorageEvent.fields.sequencer });
const locks = new WeakMap<KeyValueStore.KeyValueStore, Semaphore.Semaphore>();
export interface EventInbox {
  readonly process: <E, R>(
    event: StorageEvent,
    deliver: (input: {
      readonly event: StorageEvent;
      readonly idempotencyKey: string;
    }) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<"delivered" | "duplicate" | "out-of-order", E | StorageError, R>;
}
export class StorageEventInbox extends Context.Service<StorageEventInbox, EventInbox>()(
  "@effect-kit/storage/StorageEventInbox",
) {
  static readonly make = Effect.fnUntraced(function* (input: {
    readonly namespace: string;
    readonly ownership: "exclusive";
  }) {
    const policy = yield* Schema.decodeUnknownEffect(
      Schema.Struct({
        namespace: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,128}$/u)),
        ownership: Schema.Literal("exclusive"),
      }),
    )(input).pipe(
      Effect.mapError(() =>
        storageError("events", "inbox", { _tag: "InvalidConfiguration", field: "ownership" }),
      ),
    );
    const kvs = yield* KeyValueStore.KeyValueStore;
    const store = KeyValueStore.toSchemaStore(kvs, Record);
    let lock = locks.get(kvs);
    if (!lock) {
      lock = Semaphore.makeUnsafe(1);
      locks.set(kvs, lock);
    }
    const mutex = lock;
    return StorageEventInbox.of({
      process: <E, R>(
        raw: StorageEvent,
        deliver: (input: {
          readonly event: StorageEvent;
          readonly idempotencyKey: string;
        }) => Effect.Effect<void, E, R>,
      ) =>
        mutex.withPermit(
          Effect.gen(function* () {
            const event = yield* Schema.decodeUnknownEffect(StorageEvent)(raw).pipe(
              Effect.mapError(() =>
                storageError("events", "inbox", { _tag: "InvalidInput", field: "event" }),
              ),
            );
            if (!event.id || event.id.length > 8192)
              return yield* storageError("events", "inbox", {
                _tag: "InvalidInput",
                field: "stable-event-id-required",
              });
            const namespace = `${policy.namespace}:events:`;
            const key =
              namespace +
              encodeURIComponent(
                JSON.stringify([
                  event.provider,
                  event.ref.storeId,
                  event.ref.key,
                  event.sequencer ? "ordered" : event.id,
                ]),
              );
            const previous = yield* store
              .get(key)
              .pipe(
                Effect.mapError(() =>
                  storageError("events", "inbox", { _tag: "ProviderFailure", code: "InboxRead" }),
                ),
              );
            if (previous._tag === "Some") {
              if (previous.value.id === event.id) return "duplicate" as const;
              if (event.sequencer && previous.value.sequencer) {
                const current = event.sequencer.toUpperCase().padStart(128, "0");
                const last = previous.value.sequencer.toUpperCase().padStart(128, "0");
                if (current === last) return "duplicate" as const;
                if (current < last) return "out-of-order" as const;
              }
            }
            yield* deliver({
              event,
              idempotencyKey: JSON.stringify([
                policy.namespace,
                event.provider,
                event.ref.storeId,
                event.ref.key,
                event.id,
              ]),
            });
            // A crash after delivery and before this acknowledgement can redeliver.
            yield* store.set(key, { id: event.id, sequencer: event.sequencer }).pipe(
              Effect.mapError(() =>
                storageError("events", "inbox", {
                  _tag: "ProviderFailure",
                  code: "InboxAcknowledge",
                }),
              ),
            );
            return "delivered" as const;
          }),
        ),
    });
  });
  static layer(options: { readonly namespace: string; readonly ownership: "exclusive" }) {
    return Layer.effect(StorageEventInbox, StorageEventInbox.make(options));
  }
}

/** Authentication belongs to the delivery transport. A decoded JSON payload grants no authority. */
export const makeEventProcessor = Effect.fnUntraced(function* <
  Input,
  AuthError,
  AuthR,
  DeliverError,
  DeliverR,
>(options: {
  readonly authenticate: (
    input: Input,
  ) => Effect.Effect<ReadonlyArray<StorageEvent>, AuthError, AuthR>;
  readonly deliver: (input: {
    readonly event: StorageEvent;
    readonly idempotencyKey: string;
  }) => Effect.Effect<void, DeliverError, DeliverR>;
}) {
  const inbox = yield* StorageEventInbox;
  return (input: Input) =>
    options
      .authenticate(input)
      .pipe(
        Effect.flatMap((events) =>
          Effect.forEach(events, (event) => inbox.process(event, options.deliver)),
        ),
      );
});
