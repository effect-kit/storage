import { Clock, Context, Effect, Layer, Schema } from "effect";
import { SecureRandom } from "effect/crypto";

import { makeCollection } from "./content/collection.js";
import { contentKey, makeReferences } from "./content/references.js";
import { ContentRecord } from "./content/schema.js";
import type { ObjectMetadata } from "./object.js";
import { makeStaging } from "./representation/staging.js";
import { digestText } from "./runtime/portable-hash.js";
import { Storage } from "./storage.js";
import { Transfers, UploadSource } from "./transfers.js";
import type { UploadSourceType, TransferPolicy } from "./transfers.js";
import { deletePinned, hashObject, headOptional, pin } from "./workflows/objects.js";
import { makeRecordStore, workflowError } from "./workflows/store.js";
import type { RecordStore, WorkflowStoreOptions } from "./workflows/store.js";

export { ContentId, ContentRecord, CollectionPlan } from "./content/schema.js";
export type { WorkflowStoreOptions as ContentReferenceStoreOptions } from "./workflows/store.js";
export class ContentReferenceStore extends Context.Service<ContentReferenceStore, RecordStore>()(
  "@effect-kit/storage/ContentReferenceStore",
) {
  static make(options: WorkflowStoreOptions) {
    return makeRecordStore(options, "content");
  }
  static layer(options: WorkflowStoreOptions) {
    return Layer.effect(ContentReferenceStore, ContentReferenceStore.make(options));
  }
}
export interface ContentStoreOptions {
  readonly configurationId: string;
  readonly namespace: string;
  readonly algorithm?: "sha256";
  readonly maxBytes: number;
  readonly minimumRetentionMillis?: number;
  readonly transfer?: TransferPolicy;
}
const marker = "effect-storage-content";
const make = Effect.fnUntraced(function* (options: ContentStoreOptions) {
  const store = yield* ContentReferenceStore;
  const storage = yield* Storage;
  const random = yield* SecureRandom.SecureRandom;
  const stage = yield* makeStaging;
  const minimumRetention = options.minimumRetentionMillis ?? 86400000;
  if (
    !options.configurationId ||
    !/^[A-Za-z0-9_-]{1,100}$/u.test(options.namespace) ||
    (options.algorithm !== undefined && options.algorithm !== "sha256") ||
    !Number.isSafeInteger(options.maxBytes) ||
    options.maxBytes < 1 ||
    !Number.isSafeInteger(minimumRetention) ||
    minimumRetention < 1 ||
    !storage.capabilities.putConditions.includes("create-only")
  )
    return yield* workflowError("InvalidContentConfiguration");
  const transfers = yield* Transfers.make({ ...options.transfer, maxBytes: options.maxBytes });
  const binding = digestText(
    JSON.stringify([options.configurationId, storage.storeId, options.namespace, "sha256"]),
  );
  yield* store.exclusive(
    Effect.gen(function* () {
      const existing = yield* store.read("configuration", Schema.String);
      if (existing && existing !== binding)
        return yield* workflowError("ReferenceStoreConfigurationConflict");
      if (!existing) yield* store.write("configuration", Schema.String, binding);
    }),
  );
  const references = makeReferences(store, binding, storage, random);
  const collection = makeCollection(store, binding, storage, references, random, minimumRetention);
  const physicalKey = (record: ContentRecord) =>
    `${options.namespace}/sha256/${record.id}/${record.generation}`;
  const verify = Effect.fnUntraced(function* (record: ContentRecord, metadata: ObjectMetadata) {
    if (
      metadata.ref.storeId !== storage.storeId ||
      metadata.ref.key !== physicalKey(record) ||
      metadata.customMetadata?.[marker] !== binding
    )
      return yield* workflowError("ForeignContentObject");
    const actual = yield* hashObject(storage, metadata);
    if (actual.sha256 !== record.id || actual.size !== record.size)
      return yield* workflowError("ContentVerificationFailed");
    return metadata;
  });
  const reconcile = Effect.fnUntraced(function* (id: string) {
    const record = yield* references.load(id);
    if (record.state === "ready" || record.state === "deleted") return record;
    if (record.state === "deleting") {
      if (!record.object || (yield* references.reachable(record))) return record;
      yield* deletePinned(storage, record.object);
      const deleted: ContentRecord = { ...record, state: "deleted" };
      yield* store.write(contentKey(id), ContentRecord, deleted);
      return deleted;
    }
    const observed = yield* headOptional(storage, physicalKey(record));
    if (!observed) return record;
    const object = yield* verify(record, observed);
    const ready: ContentRecord = { ...record, state: "ready", object };
    yield* store.write(contentKey(id), ContentRecord, ready);
    return ready;
  });
  const put = <E = never, R = never>(request: {
    readonly source: UploadSourceType<E, R>;
    readonly reference?: string;
  }) =>
    store.exclusive(
      Effect.scoped(
        Effect.gen(function* () {
          const staged = yield* stage(request.source.open(), options.maxBytes);
          if (request.source.length !== undefined && request.source.length !== staged.size)
            return yield* workflowError("SourceLengthMismatch");
          const id = staged.checksum.value;
          let record = yield* store.read(contentKey(id), ContentRecord);
          if (record && record.binding !== binding)
            return yield* workflowError("ContentConfigurationConflict");
          if (record?.state === "deleting") return yield* workflowError("ContentDeletionPending");
          if (record?.state === "ready" && record.object)
            yield* verify(record, yield* storage.head({ ref: record.object.ref }));
          else {
            if (!record || record.state === "deleted") {
              const generation = (record?.generation ?? 0) + 1;
              const next: ContentRecord = {
                id,
                binding,
                generation,
                state: "writing",
                size: staged.size,
                createdAt: yield* Clock.currentTimeMillis,
                retainUntil: (yield* Clock.currentTimeMillis) + minimumRetention,
                edgeCount: record?.edgeCount ?? 0,
              };
              if (yield* headOptional(storage, physicalKey(next)))
                return yield* workflowError("UnreservedDigestKey");
              const sequence = (yield* store.read("content-sequence", Schema.Natural)) ?? 0;
              yield* store.write("content-sequence", Schema.Natural, sequence + 1);
              yield* store.write(`content-index:${sequence + 1}`, Schema.String, id);
              yield* store.write(contentKey(id), ContentRecord, next);
              record = next;
            }
            record = yield* reconcile(id);
            if (record.state !== "ready") {
              yield* transfers.upload({
                key: physicalKey(record),
                source: UploadSource.replayable(() => staged.stream(), staged.size),
                expectedChecksum: staged.checksum,
                condition: { _tag: "CreateOnly" },
                headers: {
                  contentType: "application/octet-stream",
                  customMetadata: { [marker]: binding },
                },
              });
              record = yield* reconcile(id);
            }
          }
          const object = record.object;
          if (!object || record.state !== "ready")
            return yield* workflowError("ContentOutcomeUnknown");
          record = { ...record, retainUntil: (yield* Clock.currentTimeMillis) + minimumRetention };
          yield* store.write(contentKey(id), ContentRecord, record);
          if (request.reference !== undefined) yield* references.update(request.reference, id);
          return { id, metadata: object, size: record.size, checksum: staged.checksum };
        }),
      ),
    );
  return {
    put,
    head: (id: string) =>
      store.exclusive(
        Effect.gen(function* () {
          const record = yield* references.load(id);
          if (record.state !== "ready" || !record.object)
            return yield* workflowError("ContentNotReady");
          return yield* storage.head({ ref: record.object.ref });
        }),
      ),
    get: (id: string) =>
      store.exclusive(
        Effect.gen(function* () {
          const record = yield* references.load(id);
          if (record.state !== "ready" || !record.object)
            return yield* workflowError("ContentNotReady");
          return yield* storage.get(yield* pin(record.object));
        }),
      ),
    status: (id: string) => store.exclusive(references.load(id)),
    reconcile: (id: string) => store.exclusive(reconcile(id)),
    attach: (request: {
      readonly contentId: string;
      readonly reference: string;
      readonly replace?: boolean;
    }) => store.exclusive(references.update(request.reference, request.contentId, request.replace)),
    detach: (request: { readonly reference: string }) =>
      store.exclusive(references.update(request.reference)),
    references: (id: string, page?: { readonly offset?: number; readonly limit?: number }) =>
      store.exclusive(references.inspect(id, page)),
    reconcileReference: (reference: string) => store.exclusive(references.reconcile(reference)),
    planGarbageCollection: (request: { readonly olderThan: number }) =>
      store.exclusive(collection.plan(request)),
    inspectGarbageCollection: (
      id: string,
      page?: { readonly offset?: number; readonly limit?: number },
    ) => store.exclusive(collection.inspect(id, page)),
    applyGarbageCollection: (id: string, batch?: { readonly limit?: number }) =>
      store.exclusive(collection.apply(id, batch)),
  };
});
export class ContentStore extends Context.Service<
  ContentStore,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/ContentStore") {
  static readonly make = make;
  static layer(options: ContentStoreOptions) {
    return Layer.effect(ContentStore, make(options));
  }
}
