import { Effect, Redacted, Schema, Semaphore } from "effect";
import { KeyValueStore } from "effect/persistence";

import { storageError } from "../core/errors.js";
import type { CheckpointProtection, SessionOwnership } from "../sessions/store.js";

export interface WorkflowStoreOptions {
  readonly namespace: string;
  /** Requires settled, linearizable KVS operations and exclusion through cancellation. */
  readonly ownership: "exclusive" | SessionOwnership;
  readonly protection: CheckpointProtection;
  readonly maximumRecordBytes?: number;
  readonly maximumOperationMillis?: number;
}
const locks = new WeakMap<KeyValueStore.KeyValueStore, Map<string, Semaphore.Semaphore>>();
export const workflowError = (code: string) =>
  storageError("workflow", "storage", { _tag: "ProviderFailure", code });

export const makeRecordStore = Effect.fnUntraced(function* (
  options: WorkflowStoreOptions,
  domain: string,
) {
  if (!/^[A-Za-z0-9_-]{1,100}$/u.test(options.namespace) || !options.ownership)
    return yield* workflowError("InvalidStoreConfiguration");
  const maximum = options.maximumRecordBytes ?? 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1024)
    return yield* workflowError("InvalidRecordLimit");
  const deadline = options.maximumOperationMillis ?? 30 * 60 * 1000;
  if (!Number.isSafeInteger(deadline) || deadline < 1)
    return yield* workflowError("InvalidOperationDeadline");
  const kvs = yield* KeyValueStore.KeyValueStore;
  const namespace = `${options.namespace}:${domain}`;
  let domains = locks.get(kvs);
  if (!domains) {
    domains = new Map();
    locks.set(kvs, domains);
  }
  let lock = domains.get(namespace);
  if (!lock) {
    lock = Semaphore.makeUnsafe(1);
    domains.set(namespace, lock);
  }
  const semaphore = lock;
  const key = (id: string) => `${namespace}:${encodeURIComponent(id)}`;
  return {
    exclusive: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      semaphore
        .withPermit(
          options.ownership === "exclusive" ? effect : options.ownership.withExclusive(effect),
        )
        .pipe(
          Effect.timeoutOrElse({
            duration: deadline,
            orElse: () => Effect.fail(workflowError("DeadlineExceeded")),
          }),
        ),
    read: Effect.fnUntraced(function* <S extends Schema.Constraint>(id: string, schema: S) {
      const encrypted = yield* kvs
        .get(key(id))
        .pipe(Effect.mapError(() => workflowError("ReadFailed")));
      if (encrypted === undefined) return undefined;
      if (encrypted.length > maximum * 2 + 4096) return yield* workflowError("RecordTooLarge");
      const json = yield* options.protection.open(Redacted.make(encrypted), key(id));
      if (new TextEncoder().encode(json).length > maximum)
        return yield* workflowError("RecordTooLarge");
      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(schema)))(
        json,
      ).pipe(Effect.mapError(() => workflowError("InvalidRecord")));
    }),
    write: Effect.fnUntraced(function* <S extends Schema.Constraint>(
      id: string,
      schema: S,
      value: S["Type"],
    ) {
      const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(schema))(value).pipe(
        Effect.mapError(() => workflowError("InvalidRecord")),
      );
      const json = yield* Effect.try({
        try: () => JSON.stringify(encoded),
        catch: () => workflowError("InvalidRecord"),
      });
      if (new TextEncoder().encode(json).length > maximum)
        return yield* workflowError("RecordTooLarge");
      const encrypted = yield* options.protection.seal(json, key(id));
      yield* kvs
        .set(key(id), Redacted.value(encrypted))
        .pipe(Effect.mapError(() => workflowError("WriteFailed")));
    }, Effect.uninterruptible),
  };
});
export type RecordStore = Effect.Success<ReturnType<typeof makeRecordStore>>;
