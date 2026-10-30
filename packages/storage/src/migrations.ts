import { Clock, Context, Effect, Layer, Schema, Stream } from "effect";
import { SecureRandom } from "effect/crypto";

import type { StorageError } from "./errors.js";
import {
  MigrationEntry,
  MigrationId,
  MigrationPlan,
  MigrationRequest,
} from "./migrations/schema.js";
import { ObjectMetadata } from "./object.js";
import type { ListEntry } from "./requests.js";
import { digestText } from "./runtime/portable-hash.js";
import { Storage } from "./storage.js";
import { Transfers } from "./transfers.js";
import { endpointStoreIds, TransferDriver } from "./transfers/dependencies.js";
import type { TransferEndpoint, TransferPolicy } from "./transfers/models.js";
import { buildEndpoint } from "./workflows/endpoint.js";
import { deletePinned, hashObject, headOptional } from "./workflows/objects.js";
import { makeRecordStore, workflowError } from "./workflows/store.js";
import type { RecordStore, WorkflowStoreOptions } from "./workflows/store.js";

export { MigrationEntry, MigrationId, MigrationPlan, MigrationRequest };
export type { WorkflowStoreOptions as MigrationStoreOptions } from "./workflows/store.js";
export class MigrationStore extends Context.Service<MigrationStore, RecordStore>()(
  "@effect-kit/storage/MigrationStore",
) {
  static make(options: WorkflowStoreOptions) {
    return makeRecordStore(options, "migrations");
  }
  static layer(options: WorkflowStoreOptions) {
    return Layer.effect(MigrationStore, MigrationStore.make(options));
  }
}
export interface MigrationOptions {
  readonly configurationId: string;
  readonly source: TransferEndpoint;
  readonly destination: TransferEndpoint;
  readonly transfer?: TransferPolicy;
  readonly deadlineMillis?: number;
}
const marker = "effect-storage-migration";
const rowKey = (id: string, index: number) => `${id}:entry:${index}`;
const make = Effect.fnUntraced(function* (configuration: MigrationOptions) {
  const store = yield* MigrationStore;
  const random = yield* SecureRandom.SecureRandom;
  if (
    !configuration.configurationId ||
    endpointStoreIds(configuration.source).some((id) =>
      endpointStoreIds(configuration.destination).includes(id),
    )
  )
    return yield* workflowError("InvalidEndpoints");
  const source = configuration.source;
  const destination = configuration.destination;
  const deadline = configuration.deadlineMillis ?? 30 * 60 * 1000;
  if (!Number.isSafeInteger(deadline) || deadline < 1)
    return yield* workflowError("InvalidDeadline");
  const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: deadline,
        orElse: () => Effect.fail(workflowError("DeadlineExceeded")),
      }),
    );
  const binding = digestText(
    JSON.stringify([
      configuration.configurationId,
      source.storage.storeId,
      destination.storage.storeId,
    ]),
  );
  const transfers = yield* Transfers.make(configuration.transfer).pipe(
    Effect.provideService(Storage, destination.storage),
    Effect.provideService(TransferDriver, destination),
  );
  const load = Effect.fnUntraced(function* (id: string) {
    yield* Schema.decodeUnknownEffect(MigrationId)(id).pipe(
      Effect.mapError(() => workflowError("InvalidId")),
    );
    const plan = yield* store.read(id, MigrationPlan);
    if (!plan || plan.binding !== binding) return yield* workflowError("PlanNotFound");
    return plan;
  });
  const readEntry = Effect.fnUntraced(function* (id: string, index: number) {
    const entry = yield* store.read(rowKey(id, index), MigrationEntry);
    if (!entry) return yield* workflowError("MissingManifestEntry");
    return entry;
  });
  const advance = Effect.fnUntraced(function* (plan: MigrationPlan, entry: MigrationEntry) {
    if (["completed", "skipped", "conflict"].includes(entry.state)) return entry;
    const save = (value: MigrationEntry) =>
      store.write(rowKey(plan.id, value.index), MigrationEntry, value).pipe(Effect.as(value));
    if (entry.action === "delete") {
      const sourceKey =
        plan.request.prefix +
        entry.destinationKey.slice((plan.request.destinationPrefix ?? plan.request.prefix).length);
      if (yield* headOptional(source.storage, sourceKey))
        return yield* save({ ...entry, state: "conflict" });
      yield* save({ ...entry, state: "deleting" });
      yield* deletePinned(destination.storage, entry.source);
      return yield* save({ ...entry, state: "completed" });
    }
    let row = entry;
    if (["copying", "unknown"].includes(row.state)) {
      const committed = yield* headOptional(destination.storage, row.destinationKey);
      if (!committed || committed.customMetadata?.[marker] !== `${plan.id}:${row.index}`)
        return yield* save({ ...row, state: "unknown" });
      const actual = yield* hashObject(destination.storage, committed);
      if (actual.sha256 !== row.sha256 || actual.size !== row.source.size)
        return yield* save({ ...row, state: "conflict" });
      row = yield* save({ ...row, state: "copied", destination: committed });
    }
    if (!["copied", "deleting"].includes(row.state)) {
      const current = yield* source.storage.head({ ref: row.source.ref });
      if (
        (!row.source.ref.version && current.etag !== row.source.etag) ||
        current.size !== row.source.size ||
        (row.source.ref.version &&
          JSON.stringify(current.ref.version) !== JSON.stringify(row.source.ref.version))
      )
        return yield* save({ ...row, state: "conflict" });
      row = yield* save({ ...row, state: "copying" });
      const receipt = yield* transfers.copy({
        source,
        headers: { customMetadata: { [marker]: `${plan.id}:${row.index}` } },
        ref: row.source.ref,
        destinationKey: row.destinationKey,
        condition: row.previous?.etag
          ? { _tag: "IfMatch", etag: row.previous.etag }
          : { _tag: "CreateOnly" },
      });
      if (receipt.checksum.value !== row.sha256) return yield* save({ ...row, state: "conflict" });
      row = yield* save({
        ...row,
        state: "copied",
        destination: receipt.metadata,
        strategy: receipt.strategy,
      });
    }
    if (plan.request.deleteSource) {
      // Recheck destination identity and full bytes before any source deletion, including resume.
      if (!row.destination) return yield* workflowError("MissingDestinationReceipt");
      if ((yield* hashObject(destination.storage, row.destination)).sha256 !== row.sha256)
        return yield* workflowError("DestinationVerificationFailed");
      row = yield* save({ ...row, state: "deleting" });
      yield* deletePinned(source.storage, row.source);
    }
    return yield* save({ ...row, state: "completed" });
  });
  const apply = Effect.fnUntraced(function* (
    id: string,
    options: { readonly limit?: number } = {},
  ) {
    let plan = yield* load(id);
    if (plan.state === "planning") return yield* workflowError("IncompletePlan");
    const limit = options.limit ?? 1000;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10000)
      return yield* workflowError("InvalidBatchLimit");
    plan = { ...plan, state: "applying" };
    yield* store.write(id, MigrationPlan, plan);
    let attempted = 0;
    let remaining = false;
    let partial = false;
    for (let index = 0; index < plan.count; index++) {
      const entry = yield* readEntry(id, index);
      if (["completed", "skipped"].includes(entry.state)) continue;
      if (entry.state === "conflict") {
        partial = true;
        continue;
      }
      if (attempted++ >= limit) {
        remaining = true;
        continue;
      }
      const result = yield* Effect.result(advance(plan, entry));
      if (result._tag === "Failure") {
        const latest = yield* readEntry(id, index);
        yield* store.write(rowKey(id, index), MigrationEntry, {
          ...latest,
          state: latest.state === "copying" || latest.state === "deleting" ? "unknown" : "failed",
          error: result.failure,
        });
        partial = true;
      } else if (["conflict", "unknown"].includes(result.success.state)) partial = true;
    }
    plan = { ...plan, state: remaining ? "applying" : partial ? "partial" : "completed" };
    yield* store.write(id, MigrationPlan, plan);
    return plan;
  });
  return {
    plan: <E = never, R = never>(
      raw: MigrationRequest & { readonly inventory?: Stream.Stream<ObjectMetadata, E, R> },
    ) =>
      store.exclusive(
        Effect.gen(function* () {
          const request = yield* Schema.decodeUnknownEffect(MigrationRequest)(raw).pipe(
            Effect.mapError(() => workflowError("InvalidPlanRequest")),
          );
          if (request.deleteSource && request.deleteExtraneous)
            return yield* workflowError("ConflictingDeleteModes");
          if (raw.inventory && !request.inventoryId)
            return yield* workflowError("InventoryIdentityRequired");
          const id = request.id ?? (yield* random.uuidV4());
          const existing = yield* store.read(id, MigrationPlan);
          if (existing) {
            if (
              existing.binding !== binding ||
              JSON.stringify(existing.request) !== JSON.stringify(request)
            )
              return yield* workflowError("PlanConflict");
            return existing;
          }
          let plan: MigrationPlan = {
            id,
            binding,
            request,
            state: "planning",
            count: 0,
            createdAt: yield* Clock.currentTimeMillis,
          };
          yield* store.write(id, MigrationPlan, plan);
          const destinationPrefix = request.destinationPrefix ?? request.prefix;
          const listed: Stream.Stream<ListEntry, StorageError | E, R> = raw.inventory
            ? raw.inventory.pipe(Stream.map((metadata) => ({ _tag: "Object" as const, metadata })))
            : Storage.list({ prefix: request.prefix }).pipe(
                Stream.provideService(Storage, source.storage),
              );
          yield* Stream.runForEach(listed, (entry) =>
            Effect.gen(function* () {
              if (entry._tag !== "Object") return;
              yield* Schema.encodeEffect(ObjectMetadata)(entry.metadata).pipe(
                Effect.mapError(() => workflowError("InvalidInventoryEntry")),
              );
              if (!entry.metadata.ref.key.startsWith(request.prefix))
                return yield* workflowError("InventoryOutsideScope");
              const destinationKey =
                destinationPrefix + entry.metadata.ref.key.slice(request.prefix.length);
              if (yield* store.read(`${id}:seen:${digestText(destinationKey)}`, Schema.Boolean))
                return yield* workflowError("DuplicateInventoryDestination");
              const previous = yield* headOptional(destination.storage, destinationKey);

              if (previous && request.conflict === "replace-if-unchanged" && !previous.etag)
                return yield* workflowError("DestinationCannotBeConditionallyReplaced");
              const observed = yield* source.storage.head({ ref: entry.metadata.ref });
              if (
                raw.inventory &&
                !entry.metadata.ref.version &&
                observed.etag !== entry.metadata.etag
              )
                return yield* workflowError("InventoryChanged");
              const fingerprint = yield* hashObject(source.storage, observed);
              const row: MigrationEntry = {
                index: plan.count,
                action: "copy",
                source: { ...observed, size: fingerprint.size },
                destinationKey,
                sha256: fingerprint.sha256,
                state: previous
                  ? request.conflict === "skip"
                    ? "skipped"
                    : request.conflict === "fail"
                      ? "conflict"
                      : "planned"
                  : "planned",
                ...(previous ? { previous } : {}),
              };
              yield* store.write(rowKey(id, plan.count), MigrationEntry, row);
              yield* store.write(`${id}:seen:${digestText(destinationKey)}`, Schema.Boolean, true);
              plan = { ...plan, count: plan.count + 1 };
              yield* store.write(id, MigrationPlan, plan);
            }),
          );
          if (request.deleteExtraneous) {
            yield* Stream.runForEach(
              Storage.list({ prefix: destinationPrefix }).pipe(
                Stream.provideService(Storage, destination.storage),
              ),
              (entry) =>
                Effect.gen(function* () {
                  if (
                    entry._tag !== "Object" ||
                    (yield* store.read(
                      `${id}:seen:${digestText(entry.metadata.ref.key)}`,
                      Schema.Boolean,
                    ))
                  )
                    return;
                  const row: MigrationEntry = {
                    index: plan.count,
                    action: "delete",
                    source: yield* destination.storage.head({ ref: entry.metadata.ref }),
                    destinationKey: entry.metadata.ref.key,
                    sha256: "",
                    state: "planned",
                  };
                  yield* store.write(rowKey(id, plan.count), MigrationEntry, row);
                  plan = { ...plan, count: plan.count + 1 };
                  yield* store.write(id, MigrationPlan, plan);
                }),
            );
          }
          plan = { ...plan, state: "planned" };
          yield* store.write(id, MigrationPlan, plan);
          return plan;
        }).pipe(bounded),
      ),
    status: (id: string) => store.exclusive(load(id)),
    inspect: (id: string, options: { readonly offset?: number; readonly limit?: number } = {}) =>
      store.exclusive(
        Effect.gen(function* () {
          const plan = yield* load(id);
          const offset = options.offset ?? 0;
          const limit = options.limit ?? 100;
          if (
            !Number.isSafeInteger(offset) ||
            offset < 0 ||
            !Number.isSafeInteger(limit) ||
            limit < 1 ||
            limit > 1000
          )
            return yield* workflowError("InvalidPage");
          const entries: MigrationEntry[] = [];
          for (let index = offset; index < Math.min(plan.count, offset + limit); index++)
            entries.push(yield* readEntry(id, index));
          return {
            plan,
            entries,
            ...(offset + entries.length < plan.count
              ? { nextOffset: offset + entries.length }
              : {}),
          };
        }),
      ),
    apply: (id: string, options?: { readonly limit?: number }) =>
      store.exclusive(apply(id, options).pipe(bounded)),
    resume: (id: string, options?: { readonly limit?: number }) =>
      store.exclusive(apply(id, options).pipe(bounded)),
    reconcile: (id: string, options?: { readonly limit?: number }) =>
      store.exclusive(apply(id, options).pipe(bounded)),
  };
});

export class StorageMigrations extends Context.Service<
  StorageMigrations,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/StorageMigrations") {
  static readonly make = make;
  static layer<ES, RS, ED, RD>(
    options: Omit<MigrationOptions, "source" | "destination"> & {
      readonly source: Layer.Layer<Storage | TransferDriver, ES, RS>;
      readonly destination: Layer.Layer<Storage | TransferDriver, ED, RD>;
    },
  ) {
    return Layer.effect(
      StorageMigrations,
      Effect.gen(function* () {
        const source = yield* buildEndpoint(options.source);
        const destination = yield* buildEndpoint(options.destination);
        return yield* make({ ...options, source, destination });
      }),
    );
  }
}
