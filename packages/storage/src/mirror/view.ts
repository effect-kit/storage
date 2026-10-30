import { Effect, Schema } from "effect";

import { storageError } from "../errors.js";
import type { ObjectMetadata } from "../object.js";
import type { HeadRequest, ListEntry } from "../requests.js";
import type { StorageService } from "../storage.js";
import { workflowError } from "../workflows/store.js";
import type { RecordStore } from "../workflows/store.js";
import { marker, pointerKey } from "./operations.js";
import type { makeOperations, MirrorEndpoint } from "./operations.js";
import { MirrorPointer } from "./schema.js";
import type { MirrorOperation } from "./schema.js";
const Cursor = Schema.Struct({
  binding: Schema.String,
  prefix: Schema.String,
  next: Schema.Natural,
  upper: Schema.Natural,
});

export const makeView = (
  store: RecordStore,
  binding: string,
  endpoints: ReadonlyArray<MirrorEndpoint>,
  operations: ReturnType<typeof makeOperations>,
) => {
  const storeId = `mirror:${binding}`;
  const virtual = (operation: MirrorOperation, metadata: ObjectMetadata): ObjectMetadata => {
    const customMetadata = { ...metadata.customMetadata };
    delete customMetadata[marker];
    return {
      ...metadata,
      customMetadata,
      size: operation.size,
      etag: `"mirror:${operation.id}"`,
      ref: {
        storeId,
        key: operation.key,
        version: { _tag: "OpaqueVersion", provider: "mirror", id: operation.id },
      },
    };
  };
  const current = Effect.fnUntraced(function* (target: HeadRequest, requireCommitted = true) {
    const key = "key" in target ? target.key : target.ref.key;
    const pointer = yield* store.read(pointerKey(key), MirrorPointer);
    if (!pointer || pointer.kind === "delete")
      return yield* storageError("get", "mirror", { _tag: "NotFound" });
    if (
      "ref" in target &&
      (target.ref.storeId !== storeId ||
        (target.ref.version &&
          (target.ref.version._tag !== "OpaqueVersion" ||
            target.ref.version.provider !== "mirror" ||
            target.ref.version.id !== pointer.id)))
    )
      return yield* storageError("get", "mirror", { _tag: "NotFound" });
    const operation = yield* operations.load(pointer.id);
    if (
      requireCommitted &&
      operation.outcomes.some((outcome) => outcome.required && outcome.state !== "succeeded")
    )
      return yield* workflowError("RequiredReplicationPending");
    return operation;
  });
  const resolve = Effect.fnUntraced(function* (operation: MirrorOperation) {
    for (let index = 0; index < endpoints.length; index++) {
      const endpoint = endpoints[index];
      const outcome = operation.outcomes[index];
      if (!endpoint || !outcome?.metadata || outcome.state !== "succeeded") continue;
      const observed = yield* Effect.result(
        endpoint.endpoint.storage.head({ ref: outcome.metadata.ref }),
      );
      if (observed._tag === "Success" && observed.success.customMetadata?.[marker] === operation.id)
        return { endpoint: endpoint.endpoint, metadata: observed.success };
      if (
        observed._tag === "Failure" &&
        ["AccessDenied", "PreconditionFailed", "InvalidInput"].includes(
          observed.failure.reason._tag,
        )
      )
        return yield* observed.failure;
    }
    return yield* workflowError("NoVerifiedReplicaAvailable");
  });
  const listPage: StorageService["listPage"] = (request = {}) =>
    store.exclusive(
      Effect.gen(function* () {
        const prefix = request.prefix ?? "";
        const cursor = request.cursor
          ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Cursor))(request.cursor).pipe(
              Effect.mapError(() => workflowError("InvalidCursor")),
            )
          : {
              binding,
              prefix,
              next: 1,
              upper: (yield* store.read("sequence", Schema.Natural)) ?? 0,
            };
        if (cursor.binding !== binding || cursor.prefix !== prefix)
          return yield* workflowError("ForeignCursor");
        const entries: ListEntry[] = [];
        let index = cursor.next;
        let scanned = 0;
        while (
          index <= cursor.upper &&
          entries.length < (request.limit ?? 100) &&
          scanned++ < 1000
        ) {
          const id = yield* store.read(`index:${index++}`, Schema.String);
          if (!id) continue;
          const operation = yield* operations.load(id);
          const pointer = yield* store.read(pointerKey(operation.key), MirrorPointer);
          if (pointer?.id !== id || pointer.kind === "delete" || !operation.key.startsWith(prefix))
            continue;
          const source = yield* resolve(operation);
          entries.push({ _tag: "Object", metadata: virtual(operation, source.metadata) });
        }
        return {
          entries,
          ...(index <= cursor.upper ? { cursor: JSON.stringify({ ...cursor, next: index }) } : {}),
        };
      }),
    );
  return { virtual, current, resolve, listPage };
};
