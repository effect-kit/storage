import { Clock, Context, Effect, Layer, Redacted, Schema, Semaphore } from "effect";
import { KeyValueStore } from "effect/persistence";

import type { StorageError } from "../errors.js";
import { storageError } from "../errors.js";
import { SessionCheckpoint, SessionId } from "./checkpoint.js";

export const sessionError = (code: string, unknown = false) =>
  storageError(
    "sessions",
    "session",
    { _tag: "ProviderFailure", code },
    unknown ? "unknown" : "not-started",
  );

export interface CheckpointProtection {
  readonly seal: (
    plaintext: string,
    associatedData: string,
  ) => Effect.Effect<Redacted.Redacted<string>, StorageError>;
  readonly open: (
    ciphertext: Redacted.Redacted<string>,
    associatedData: string,
  ) => Effect.Effect<string, StorageError>;
}
/** Implementations must exclude every competing owner for the entire supplied effect. */
export interface SessionOwnership {
  readonly withExclusive: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | StorageError, R>;
}
export interface SessionStoreOptions {
  readonly namespace: string;
  readonly protection: CheckpointProtection;
  /** A promise by the application: no other process accesses this namespace concurrently. */
  readonly ownership: "exclusive" | SessionOwnership;
  readonly maximumCheckpointBytes?: number;
}
const locks = new WeakMap<KeyValueStore.KeyValueStore, Semaphore.Semaphore>();
const ProtectedRecord = Schema.Struct({
  version: Schema.Literal(1),
  ciphertext: Schema.Redacted(Schema.String),
});
const json = Schema.toCodecJson(SessionCheckpoint);
const envelope = Schema.toCodecJson(ProtectedRecord);

export class UploadSessionStore extends Context.Service<
  UploadSessionStore,
  {
    readonly namespace: string;
    readonly exclusive: SessionOwnership["withExclusive"];
    readonly read: (id: string) => Effect.Effect<SessionCheckpoint | undefined, StorageError>;
    readonly remove: (id: string, revision: number) => Effect.Effect<void, StorageError>;
    /** Engine-internal: call only inside exclusive ownership. Revision is checked under that ownership. */
    readonly write: (
      checkpoint: SessionCheckpoint,
      expectedRevision: number | undefined,
    ) => Effect.Effect<SessionCheckpoint, StorageError>;
  }
>()("@effect-kit/storage/UploadSessionStore") {
  static readonly make = Effect.fnUntraced(function* (options: SessionStoreOptions) {
    const kvs = yield* KeyValueStore.KeyValueStore;
    yield* Schema.decodeUnknownEffect(SessionId)(options.namespace).pipe(
      Effect.mapError(() => sessionError("InvalidNamespace")),
    );
    const maximum = options.maximumCheckpointBytes ?? 32 * 1024 * 1024;
    if (!Number.isSafeInteger(maximum) || maximum < 1 || !options.ownership)
      return yield* sessionError("InvalidStorePolicy");
    let lock = locks.get(kvs);
    if (!lock) {
      lock = Semaphore.makeUnsafe(1);
      locks.set(kvs, lock);
    }
    const mutex = lock;
    const envelopes = KeyValueStore.toSchemaStore(kvs, envelope);
    const keyFor = (id: string) => `${options.namespace}:${id}`;
    const read = Effect.fnUntraced(function* (id: string) {
      yield* Schema.decodeUnknownEffect(SessionId)(id).pipe(
        Effect.mapError(() => sessionError("InvalidSessionId")),
      );
      const record = yield* envelopes
        .get(keyFor(id))
        .pipe(Effect.mapError(() => sessionError("CheckpointRead")));
      if (record._tag === "None") return undefined;
      if (Redacted.value(record.value.ciphertext).length > maximum * 2 + 4096)
        return yield* sessionError("CheckpointLimit");
      const text = yield* options.protection.open(record.value.ciphertext, keyFor(id));
      if (new TextEncoder().encode(text).length > maximum)
        return yield* sessionError("CheckpointLimit");
      const checkpoint = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(json))(text).pipe(
        Effect.mapError(() => sessionError("InvalidCheckpoint")),
      );
      if (checkpoint.id !== id || checkpoint.namespace !== options.namespace)
        return yield* sessionError("ForeignCheckpoint");
      return checkpoint;
    });
    return UploadSessionStore.of({
      namespace: options.namespace,
      exclusive: (effect) =>
        mutex.withPermit(
          options.ownership === "exclusive" ? effect : options.ownership.withExclusive(effect),
        ),
      read,
      remove: Effect.fnUntraced(function* (id, revision) {
        const previous = yield* read(id);
        if (!previous || previous.revision !== revision)
          return yield* sessionError("RevisionConflict");
        yield* envelopes.remove(keyFor(id)).pipe(
          Effect.uninterruptible,
          Effect.mapError(() => sessionError("CheckpointDelete")),
        );
      }),
      write: Effect.fnUntraced(function* (checkpoint, expectedRevision) {
        if (checkpoint.namespace !== options.namespace)
          return yield* sessionError("ForeignCheckpoint");
        const previous = yield* read(checkpoint.id);
        if (previous?.revision !== expectedRevision) return yield* sessionError("RevisionConflict");
        const updated = {
          ...checkpoint,
          revision: (expectedRevision ?? -1) + 1,
          updatedAt: yield* Clock.currentTimeMillis,
        };
        const text = yield* Schema.encodeEffect(Schema.fromJsonString(json))(updated).pipe(
          Effect.mapError(() => sessionError("InvalidCheckpoint")),
        );
        if (new TextEncoder().encode(text).length > maximum)
          return yield* sessionError("CheckpointLimit");
        const ciphertext = yield* options.protection.seal(text, keyFor(checkpoint.id));
        yield* envelopes.set(keyFor(checkpoint.id), { version: 1, ciphertext }).pipe(
          Effect.uninterruptible,
          Effect.mapError(() => sessionError("CheckpointWrite")),
        );
        return updated;
      }),
    });
  });

  static layer(options: SessionStoreOptions) {
    return Layer.effect(UploadSessionStore, UploadSessionStore.make(options));
  }
}
