import { Context, Effect, Layer, Redacted, Schema, Semaphore } from "effect";
import { KeyValueStore } from "effect/persistence";

import type { StorageError } from "../core/errors.js";
import type { CheckpointProtection, SessionOwnership } from "../sessions/store.js";
import { UploadId, UploadRecord, failUpload } from "./schema.js";

export interface UploadStoreOptions {
  readonly namespace: string;
  readonly ownership: "exclusive" | SessionOwnership;
  readonly protection: CheckpointProtection;
  readonly maximumRecordBytes?: number;
}
export class UploadStore extends Context.Service<
  UploadStore,
  {
    readonly exclusive: SessionOwnership["withExclusive"];
    readonly read: (
      id: string,
    ) => Effect.Effect<UploadRecord | undefined, StorageError | ReturnType<typeof failUpload>>;
    readonly write: (
      record: UploadRecord,
    ) => Effect.Effect<void, StorageError | ReturnType<typeof failUpload>>;
  }
>()("@effect-kit/storage/UploadStore") {
  static readonly make = Effect.fnUntraced(function* (options: UploadStoreOptions) {
    if (!/^[A-Za-z0-9_-]{1,100}$/u.test(options.namespace) || !options.ownership)
      return yield* failUpload("BadRequest");
    const limit = options.maximumRecordBytes ?? 1024 * 1024;
    if (!Number.isSafeInteger(limit) || limit < 1024) return yield* failUpload("BadRequest");
    const kvs = yield* KeyValueStore.KeyValueStore;
    let semaphore = locks.get(kvs);
    if (!semaphore) {
      semaphore = Semaphore.makeUnsafe(1);
      locks.set(kvs, semaphore);
    }
    const lock = semaphore;
    const key = (id: string) => `${options.namespace}:upload-intent:${id}`;
    return UploadStore.of({
      exclusive: (program) =>
        lock.withPermit(
          options.ownership === "exclusive" ? program : options.ownership.withExclusive(program),
        ),
      read: Effect.fnUntraced(function* (id) {
        yield* Schema.decodeUnknownEffect(UploadId)(id).pipe(
          Effect.mapError(() => failUpload("BadRequest")),
        );
        const encrypted = yield* kvs
          .get(key(id))
          .pipe(Effect.mapError(() => failUpload("Unavailable")));
        if (encrypted === undefined) return undefined;
        if (encrypted.length > limit * 2 + 4096) return yield* failUpload("Unavailable");
        const json = yield* options.protection.open(Redacted.make(encrypted), key(id));
        if (new TextEncoder().encode(json).length > limit) return yield* failUpload("Unavailable");
        const record = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(Schema.toCodecJson(UploadRecord)),
        )(json).pipe(Effect.mapError(() => failUpload("Unavailable")));
        if (record.id !== id) return yield* failUpload("Unavailable");
        return record;
      }),
      write: Effect.fnUntraced(function* (record) {
        const value = yield* Schema.encodeEffect(Schema.toCodecJson(UploadRecord))(record).pipe(
          Effect.mapError(() => failUpload("BadRequest")),
        );
        const json = yield* Effect.try({
          try: () => JSON.stringify(value),
          catch: () => failUpload("BadRequest"),
        });
        if (new TextEncoder().encode(json).length > limit) return yield* failUpload("BadRequest");
        const encrypted = yield* options.protection.seal(json, key(record.id));
        yield* kvs
          .set(key(record.id), Redacted.value(encrypted))
          .pipe(Effect.mapError(() => failUpload("Unavailable")));
      }),
    });
  });
  static layer(options: UploadStoreOptions) {
    return Layer.effect(UploadStore, UploadStore.make(options));
  }
}
const locks = new WeakMap<KeyValueStore.KeyValueStore, Semaphore.Semaphore>();
