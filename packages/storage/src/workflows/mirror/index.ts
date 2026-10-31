import { Context, Effect, Layer, Schema, Stream } from "effect";
import type { Scope } from "effect";
import { SecureRandom } from "effect/crypto";

import { makeStaging } from "../../composition/representation/staging.js";
import { StorageError, storageError } from "../../core/errors.js";
import { ObjectHeaders } from "../../core/object.js";
import { DeleteRequest, PutRequest } from "../../core/requests.js";
import type { GetResult } from "../../core/requests.js";
import { Storage } from "../../core/storage.js";
import { digestText } from "../../runtime/portable-hash.js";
import { endpointStoreIds, TransferDriver } from "../../transfers/dependencies.js";
import { Transfers } from "../../transfers/index.js";
import type { TransferEndpoint, TransferPolicy } from "../../transfers/models.js";
import { buildEndpoint } from "../endpoint.js";
import { pin } from "../objects.js";
import { makeRecordStore, workflowError } from "../store.js";
import type { RecordStore, WorkflowStoreOptions } from "../store.js";
import { makeOperations, marker, pointerKey } from "./operations.js";
import { MirrorOperation, MirrorPointer, MirrorRepairPlan } from "./schema.js";
import { makeView } from "./view.js";

export { MirrorOperation, MirrorRepairPlan };
export type { WorkflowStoreOptions as MirrorStoreOptions } from "../store.js";
export class MirrorStore extends Context.Service<MirrorStore, RecordStore>()(
  "@effect-kit/storage/MirrorStore",
) {
  static make(options: WorkflowStoreOptions) {
    return makeRecordStore(options, "mirror");
  }
  static layer(options: WorkflowStoreOptions) {
    return Layer.effect(MirrorStore, MirrorStore.make(options));
  }
}
export interface MirrorOptions {
  readonly configurationId: string;
  readonly primary: TransferEndpoint;
  readonly replicas: ReadonlyArray<{
    readonly storage: TransferEndpoint;
    readonly required: boolean;
  }>;
  readonly maxBytes: number;
  readonly transfer?: TransferPolicy;
}
const ensureRequired = (operation: MirrorOperation): Effect.Effect<void, StorageError> =>
  operation.outcomes.every((outcome) => !outcome.required || outcome.state === "succeeded")
    ? Effect.void
    : Effect.fail(
        new StorageError({
          operation: operation.kind,
          provider: "mirror",
          reason: { _tag: "PartialFailure" },
          mutationOutcome: "partial",
          requestId: operation.id,
        }),
      );

const make = Effect.fnUntraced(function* (configuration: MirrorOptions) {
  const store = yield* MirrorStore;
  const random = yield* SecureRandom.SecureRandom;
  const stage = yield* makeStaging;
  const selected = [
    { endpoint: configuration.primary, required: true },
    ...configuration.replicas.map((replica) => ({
      endpoint: replica.storage,
      required: replica.required,
    })),
  ];
  const physicalStores = selected.flatMap(({ endpoint }) => endpointStoreIds(endpoint));
  if (
    !configuration.configurationId ||
    !Number.isSafeInteger(configuration.maxBytes) ||
    configuration.maxBytes < 1 ||
    selected.length < 2 ||
    selected.length > 9 ||
    new Set(physicalStores).size !== physicalStores.length
  )
    return yield* workflowError("InvalidMirrorConfiguration");
  const endpoints = yield* Effect.forEach(selected, (value) =>
    Transfers.make(configuration.transfer).pipe(
      Effect.provideService(Storage, value.endpoint.storage),
      Effect.provideService(TransferDriver, value.endpoint),
      Effect.map((transfers) => ({ ...value, transfers })),
    ),
  );
  const maximumObjectBytes = Math.min(
    configuration.maxBytes,
    configuration.primary.maximumSingleUploadBytes,
    ...selected.filter((entry) => entry.required).map((entry) => entry.endpoint.maximumObjectBytes),
  );
  const putConditions = (["create-only", "if-match"] as const).filter((condition) =>
    selected.every(
      (entry) =>
        !entry.required || entry.endpoint.storage.capabilities.putConditions.includes(condition),
    ),
  );
  const binding = digestText(
    JSON.stringify([
      configuration.configurationId,
      endpoints.map((entry) => [entry.endpoint.storage.storeId, entry.required]),
    ]),
  );
  yield* store.exclusive(
    Effect.gen(function* () {
      const previous = yield* store.read("configuration", Schema.String);
      if (previous && previous !== binding)
        return yield* workflowError("MirrorStoreConfigurationConflict");
      if (!previous) yield* store.write("configuration", Schema.String, binding);
    }),
  );
  const storeId = `mirror:${binding}`;
  const operations = makeOperations(store, binding, endpoints);
  const { virtual, current, resolve, listPage } = makeView(store, binding, endpoints, operations);
  const write = <E = never, R = never>(request: PutRequest<E, R> & { readonly id?: string }) =>
    store.exclusive(
      Effect.scoped(
        Effect.gen(function* () {
          if (
            request.condition &&
            !putConditions.includes(
              request.condition._tag === "CreateOnly" ? "create-only" : "if-match",
            )
          )
            return yield* storageError(
              "put",
              "mirror",
              { _tag: "Unsupported", capability: "write-condition" },
              "not-started",
            );
          yield* Schema.decodeUnknownEffect(PutRequest)(request).pipe(
            Effect.mapError(() => workflowError("InvalidWrite")),
          );
          if (!(request.body instanceof Uint8Array) && !Stream.isStream(request.body))
            return yield* workflowError("InvalidBody");
          const headers = yield* Schema.decodeUnknownEffect(ObjectHeaders)(request).pipe(
            Effect.mapError(() => workflowError("InvalidHeaders")),
          );
          if (headers.customMetadata?.[marker] !== undefined)
            return yield* workflowError("ReservedMetadata");
          const staged = yield* stage(
            request.body instanceof Uint8Array ? Stream.succeed(request.body) : request.body,
            Math.min(maximumObjectBytes, request.maxBytes ?? maximumObjectBytes),
          );
          if (request.contentLength !== undefined && request.contentLength !== staged.size)
            return yield* workflowError("LengthMismatch");
          const id = request.id ?? (yield* random.uuidV4());
          if (!/^[A-Za-z0-9_-]{1,128}$/u.test(id))
            return yield* workflowError("InvalidOperationId");
          const requestHash = digestText(
            JSON.stringify([
              request.key,
              headers,
              request.condition,
              staged.size,
              staged.checksum.value,
            ]),
          );
          const existing = yield* store.read(`operation:${id}`, MirrorOperation);
          if (existing) {
            if (
              existing.binding !== binding ||
              existing.key !== request.key ||
              existing.kind !== "put" ||
              existing.requestHash !== requestHash
            )
              return yield* workflowError("OperationConflict");
            const repaired = yield* operations.repair(existing);
            if (repaired.outcomes[0]?.state === "succeeded" || repaired.state === "superseded")
              return {
                operation: repaired,
                metadata: repaired.outcomes[0]?.metadata
                  ? virtual(repaired, repaired.outcomes[0].metadata)
                  : undefined,
              };
          }
          const previous = yield* store.read(pointerKey(request.key), MirrorPointer);
          if (existing && previous && previous.generation > existing.generation)
            return yield* workflowError("SupersededOperation");
          const previousOperation =
            previous?.kind === "put" ? yield* operations.load(previous.id) : undefined;
          if (
            request.condition?._tag === "CreateOnly" &&
            previousOperation &&
            previousOperation.id !== id
          )
            return yield* storageError(
              "put",
              "mirror",
              { _tag: "PreconditionFailed" },
              "not-started",
            );
          if (
            request.condition?._tag === "IfMatch" &&
            (!previousOperation || request.condition.etag !== `"mirror:${previousOperation.id}"`)
          )
            return yield* storageError(
              "put",
              "mirror",
              { _tag: "PreconditionFailed" },
              "not-started",
            );
          const operation =
            existing ??
            (yield* operations.create(
              id,
              request.key,
              "put",
              staged.size,
              staged.checksum.value,
              requestHash,
            ));
          const previousMetadata = previousOperation?.outcomes[0]?.metadata;
          const physical = yield* Effect.result(
            configuration.primary.storage.put({
              ...headers,
              key: request.key,
              body: staged.stream(),
              contentLength: staged.size,
              maxBytes: maximumObjectBytes,
              customMetadata: { ...headers.customMetadata, [marker]: id },
              condition: previousMetadata?.etag
                ? { _tag: "IfMatch", etag: previousMetadata.etag }
                : { _tag: "CreateOnly" },
            }),
          );
          if (physical._tag === "Failure")
            yield* operations.save({
              ...operation,
              outcomes: operation.outcomes.map((outcome, index) =>
                index === 0 ? { ...outcome, state: "unknown", error: physical.failure } : outcome,
              ),
            });
          const repaired = yield* operations.repair(yield* operations.load(operation.id));
          return {
            operation: repaired,
            metadata: repaired.outcomes[0]?.metadata
              ? virtual(repaired, repaired.outcomes[0].metadata)
              : undefined,
          };
        }),
      ),
    );
  const remove = (request: DeleteRequest) =>
    store.exclusive(
      Effect.gen(function* () {
        yield* Schema.decodeUnknownEffect(DeleteRequest)(request).pipe(
          Effect.mapError(() => workflowError("InvalidDelete")),
        );
        const key = "key" in request ? request.key : request.ref.key;
        if ("ref" in request && request.ref.storeId !== storeId)
          return yield* workflowError("ForeignReference");
        const previous = yield* store.read(pointerKey(key), MirrorPointer);
        if (previous?.kind === "delete")
          return yield* operations.repair(yield* operations.load(previous.id));
        const operation = yield* current(request, false);
        if (request.condition && request.condition.etag !== `"mirror:${operation.id}"`)
          return yield* storageError(
            "delete",
            "mirror",
            { _tag: "PreconditionFailed" },
            "not-started",
          );
        const tombstone = yield* operations.create(
          yield* random.uuidV4(),
          operation.key,
          "delete",
          0,
          "",
        );
        yield* operations.activate(tombstone);
        return yield* operations.repair(tombstone);
      }),
    );
  const storage = Storage.make({
    provider: "mirror",
    storeId,
    capabilities: {
      putConditions,
      getConditions: ["if-match", "if-none-match"],
      deleteConditions: ["if-match"],
      versionedReads: "current-only",
      ranges: configuration.primary.storage.capabilities.ranges.filter((range) =>
        endpoints.every((endpoint) =>
          endpoint.endpoint.storage.capabilities.ranges.includes(range),
        ),
      ),
      list: { maxPageSize: 1000, delimiter: false, cursorPersistence: "provider", snapshot: false },
    },
    put: (request) =>
      write(request).pipe(
        Effect.flatMap((receipt) =>
          ensureRequired(receipt.operation).pipe(
            Effect.flatMap(() =>
              receipt.metadata
                ? Effect.succeed(receipt.metadata)
                : Effect.fail(workflowError("MissingPrimaryReceipt")),
            ),
          ),
        ),
      ),
    delete: (request) => remove(request).pipe(Effect.flatMap(ensureRequired)),
    head: (request) =>
      store.exclusive(
        Effect.gen(function* () {
          const operation = yield* current(request);
          const source = yield* resolve(operation);
          return virtual(operation, source.metadata);
        }),
      ),
    get: (request): Effect.Effect<GetResult, StorageError, Scope.Scope> =>
      store.exclusive(
        Effect.gen(function* () {
          const operation = yield* current(request);
          const source = yield* resolve(operation);
          const metadata = virtual(operation, source.metadata);
          if (
            request.condition?._tag === "IfMatch" &&
            request.condition.etag !== metadata.etag &&
            request.condition.etag !== "*"
          )
            return yield* storageError("get", "mirror", { _tag: "PreconditionFailed" });
          if (
            request.condition?._tag === "IfNoneMatch" &&
            (request.condition.etag === metadata.etag || request.condition.etag === "*")
          )
            return { _tag: "NotModified", metadata };
          const value = yield* source.endpoint.storage.get({
            ...(yield* pin(source.metadata)),
            ...(request.range ? { range: request.range } : {}),
          });
          if (value._tag !== "Object") return yield* workflowError("ReplicaChanged");
          return { ...value, metadata };
        }),
      ),
    listPage,
  });
  const applyRepair = Effect.fnUntraced(function* (id: string, limit = 100) {
    let plan = yield* store.read(`repair:${id}`, MirrorRepairPlan);
    if (
      !plan ||
      plan.binding !== binding ||
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > 1000
    )
      return yield* workflowError("InvalidRepairPlan");
    if (plan.state === "partial") plan = { ...plan, offset: 0, unresolved: 0 };
    for (let attempted = 0; plan.offset < plan.count && attempted < limit; attempted++) {
      const operationId = yield* store.read(`repair:${id}:${plan.offset}`, Schema.String);
      const repaired = operationId
        ? yield* operations.repair(yield* operations.load(operationId))
        : undefined;
      const unresolved: number =
        plan.unresolved + (repaired && ["pending", "partial"].includes(repaired.state) ? 1 : 0);
      plan = {
        id: plan.id,
        binding: plan.binding,
        count: plan.count,
        prefix: plan.prefix,
        unresolved,
        offset: plan.offset + 1,
        state: plan.offset + 1 >= plan.count ? (unresolved ? "partial" : "completed") : "applying",
      };
      yield* store.write(`repair:${id}`, MirrorRepairPlan, plan);
    }
    return plan;
  });
  return {
    storage,
    maximumObjectBytes,
    write,
    remove,
    status: (id: string) => store.exclusive(operations.load(id)),
    statusFor: (key: string) =>
      store.exclusive(
        Effect.gen(function* () {
          const pointer = yield* store.read(pointerKey(key), MirrorPointer);
          return pointer ? yield* operations.load(pointer.id) : undefined;
        }),
      ),
    planRepair: (request: { readonly prefix: string }) =>
      store.exclusive(
        Effect.gen(function* () {
          yield* Schema.decodeUnknownEffect(Schema.String)(request.prefix).pipe(
            Effect.mapError(() => workflowError("InvalidPrefix")),
          );
          const id = yield* random.uuidV4();
          let count = 0;
          const upper = (yield* store.read("sequence", Schema.Natural)) ?? 0;
          for (let index = 1; index <= upper; index++) {
            const operationId = yield* store.read(`index:${index}`, Schema.String);
            if (!operationId) continue;
            const operation = yield* operations.load(operationId);
            const pointer = yield* store.read(pointerKey(operation.key), MirrorPointer);
            if (
              !operation.key.startsWith(request.prefix) ||
              (pointer && pointer.generation > operation.generation)
            )
              continue;
            yield* store.write(`repair:${id}:${count++}`, Schema.String, operation.id);
          }
          const plan: MirrorRepairPlan = {
            id,
            binding,
            prefix: request.prefix,
            count,
            unresolved: 0,
            offset: 0,
            state: count ? "planned" : "completed",
          };
          yield* store.write(`repair:${id}`, MirrorRepairPlan, plan);
          return plan;
        }),
      ),
    inspectRepair: (id: string, page: { readonly offset?: number; readonly limit?: number } = {}) =>
      store.exclusive(
        Effect.gen(function* () {
          const plan = yield* store.read(`repair:${id}`, MirrorRepairPlan);
          if (!plan || plan.binding !== binding) return yield* workflowError("InvalidRepairPlan");
          const offset = page.offset ?? 0;
          const limit = page.limit ?? 100;
          if (
            !Number.isSafeInteger(offset) ||
            offset < 0 ||
            !Number.isSafeInteger(limit) ||
            limit < 1 ||
            limit > 1000
          )
            return yield* workflowError("InvalidPage");
          const entries: MirrorOperation[] = [];
          for (let index = offset; index < Math.min(plan.count, offset + limit); index++) {
            const operationId = yield* store.read(`repair:${id}:${index}`, Schema.String);
            if (operationId) entries.push(yield* operations.load(operationId));
          }
          return {
            plan,
            entries,
            ...(offset + entries.length < plan.count
              ? { nextOffset: offset + entries.length }
              : {}),
          };
        }),
      ),
    applyRepair: (id: string, options?: { readonly limit?: number }) =>
      store.exclusive(applyRepair(id, options?.limit)),
  };
});

export class StorageMirror extends Context.Service<
  StorageMirror,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/StorageMirror") {
  static readonly make = make;
  static layer<EP, RP, ER, RR>(
    options: Omit<MirrorOptions, "primary" | "replicas"> & {
      readonly primary: Layer.Layer<Storage | TransferDriver, EP, RP>;
      readonly replicas: ReadonlyArray<{
        readonly storage: Layer.Layer<Storage | TransferDriver, ER, RR>;
        readonly required: boolean;
      }>;
    },
  ) {
    return Layer.effectContext(
      Effect.gen(function* () {
        const primary = yield* buildEndpoint(options.primary);
        const replicas = yield* Effect.forEach(options.replicas, (replica) =>
          buildEndpoint(replica.storage).pipe(
            Effect.map((endpoint) => ({
              storage: endpoint,
              required: replica.required,
            })),
          ),
        );
        const mirror = yield* make({ ...options, primary, replicas });
        return Context.make(StorageMirror, mirror).pipe(
          Context.add(Storage, mirror.storage),
          Context.add(TransferDriver, {
            storage: mirror.storage,
            maximumObjectBytes: mirror.maximumObjectBytes,
            maximumSingleUploadBytes: mirror.maximumObjectBytes,
            transportOverheadBytes: primary.transportOverheadBytes,
            underlyingStoreId: primary.underlyingStoreId ?? primary.storage.storeId,
            underlyingStoreIds: [
              ...endpointStoreIds(primary),
              ...replicas.flatMap((replica) => endpointStoreIds(replica.storage)),
            ],
          }),
        );
      }),
    );
  }
}
