import { Clock, Effect, Schema } from "effect";

import type { ObjectMetadata } from "../../core/object.js";
import { digestText } from "../../runtime/portable-hash.js";
import type { TransfersService, TransferEndpoint } from "../../transfers/models.js";
import { deletePinned, hashObject, headOptional } from "../objects.js";
import { workflowError } from "../store.js";
import type { RecordStore } from "../store.js";
import { MirrorOperation, MirrorPointer } from "./schema.js";

export interface MirrorEndpoint {
  readonly endpoint: TransferEndpoint;
  readonly required: boolean;
  readonly transfers: TransfersService;
}
export const marker = "effect-storage-mirror";
export const pointerKey = (key: string) => `key:${digestText(key)}`;
export const makeOperations = (
  store: RecordStore,
  binding: string,
  endpoints: ReadonlyArray<MirrorEndpoint>,
) => {
  const load = Effect.fnUntraced(function* (id: string) {
    const operation = yield* store.read(`operation:${id}`, MirrorOperation);
    if (!operation || operation.binding !== binding)
      return yield* workflowError("MirrorOperationNotFound");
    return operation;
  });
  const save = (operation: MirrorOperation) =>
    store.write(`operation:${operation.id}`, MirrorOperation, operation);
  const next = Effect.gen(function* () {
    const count = (yield* store.read("sequence", Schema.Natural)) ?? 0;
    yield* store.write("sequence", Schema.Natural, count + 1);
    return count + 1;
  });
  const owned = Effect.fnUntraced(function* (metadata: ObjectMetadata) {
    const id = metadata.customMetadata?.[marker];
    if (!id) return yield* workflowError("ForeignMirrorObject");
    const operation = yield* load(id);
    if (operation.key !== metadata.ref.key) return yield* workflowError("ForeignMirrorObject");
    return operation;
  });
  const activate = Effect.fnUntraced(function* (operation: MirrorOperation) {
    const previous = yield* store.read(pointerKey(operation.key), MirrorPointer);
    if (previous && previous.generation > operation.generation) return false;
    yield* store.write(pointerKey(operation.key), MirrorPointer, {
      id: operation.id,
      kind: operation.kind,
      generation: operation.generation,
    });
    return true;
  });
  const repair = Effect.fnUntraced(function* (input: MirrorOperation) {
    let operation = input;
    const pointer = yield* store.read(pointerKey(operation.key), MirrorPointer);
    if (pointer && pointer.generation > operation.generation) {
      operation = { ...operation, state: "superseded" };
      yield* save(operation);
      return operation;
    }
    const primary = endpoints[0];
    if (!primary) return yield* workflowError("MissingPrimary");
    let authoritative: ObjectMetadata | undefined;
    if (operation.kind === "put") {
      authoritative = yield* headOptional(primary.endpoint.storage, operation.key);
      if (!authoritative) {
        for (const replica of endpoints.slice(1)) {
          const candidate = yield* headOptional(replica.endpoint.storage, operation.key);
          if (!candidate || candidate.customMetadata?.[marker] !== operation.id) continue;
          if ((yield* hashObject(replica.endpoint.storage, candidate)).sha256 !== operation.sha256)
            continue;
          authoritative = (yield* primary.transfers.copy({
            source: replica.endpoint,
            ref: candidate.ref,
            destinationKey: operation.key,
            condition: { _tag: "CreateOnly" },
          })).metadata;
          break;
        }
      }
      if (!authoritative || authoritative.customMetadata?.[marker] !== operation.id) {
        operation = { ...operation, state: "partial" };
        yield* save(operation);
        return operation;
      }
      const verified = yield* hashObject(primary.endpoint.storage, authoritative);
      if (verified.sha256 !== operation.sha256 || verified.size !== operation.size)
        return yield* workflowError("PrimaryVerificationFailed");
      if (!(yield* activate(operation))) return operation;
      operation = {
        ...operation,
        outcomes: operation.outcomes.map((outcome, index) =>
          index === 0 ? { ...outcome, state: "succeeded", metadata: authoritative } : outcome,
        ),
      };
      yield* save(operation);
    } else if (!(yield* activate(operation))) return operation;

    for (let index = operation.kind === "put" ? 1 : 0; index < endpoints.length; index++) {
      const selected = endpoints[index];
      const outcome = operation.outcomes[index];
      if (!selected || !outcome) continue;
      const attempt = yield* Effect.result(
        Effect.gen(function* () {
          const current = yield* headOptional(selected.endpoint.storage, operation.key);
          if (operation.kind === "delete") {
            if (current) {
              const owner = yield* owned(current);
              if (owner.generation > operation.generation)
                return yield* workflowError("NewerMirrorObject");
              yield* deletePinned(selected.endpoint.storage, current);
            }
            return undefined;
          }
          if (!authoritative) return yield* workflowError("MissingPrimaryReceipt");
          if (current) {
            const owner = yield* owned(current);
            if (owner.generation > operation.generation)
              return yield* workflowError("NewerMirrorObject");
            if (owner.id === operation.id) {
              if (
                (yield* hashObject(selected.endpoint.storage, current)).sha256 !== operation.sha256
              )
                return yield* workflowError("ReplicaVerificationFailed");
              return current;
            }
            if (!current.etag) return yield* workflowError("UnconditionalReplicaOverwriteRejected");
          }
          return (yield* selected.transfers.copy({
            source: primary.endpoint,
            ref: authoritative.ref,
            destinationKey: operation.key,
            condition: current?.etag
              ? { _tag: "IfMatch", etag: current.etag }
              : { _tag: "CreateOnly" },
          })).metadata;
        }),
      );
      operation = {
        ...operation,
        outcomes: operation.outcomes.map((previous, position) =>
          position !== index
            ? previous
            : attempt._tag === "Success"
              ? {
                  storeId: previous.storeId,
                  required: previous.required,
                  state: "succeeded",
                  ...(attempt.success ? { metadata: attempt.success } : {}),
                }
              : { ...previous, state: "unknown", error: attempt.failure },
        ),
      };
      yield* save(operation);
    }
    operation = {
      ...operation,
      state: operation.outcomes.every((outcome) => outcome.state === "succeeded")
        ? "completed"
        : "partial",
    };
    yield* save(operation);
    return operation;
  });
  const create = Effect.fnUntraced(function* (
    id: string,
    key: string,
    kind: MirrorOperation["kind"],
    size: number,
    sha256: string,
    requestHash?: string,
  ) {
    const generation = yield* next;
    const operation: MirrorOperation = {
      id,
      binding,
      generation,
      key,
      kind,
      size,
      sha256,
      state: "pending",
      createdAt: yield* Clock.currentTimeMillis,
      ...(requestHash ? { requestHash } : {}),
      outcomes: endpoints.map((selected) => ({
        storeId: selected.endpoint.storage.storeId,
        required: selected.required,
        state: "pending",
      })),
    };
    yield* save(operation);
    yield* store.write(`index:${generation}`, Schema.String, id);
    return operation;
  });
  return { load, save, owned, activate, repair, create };
};
