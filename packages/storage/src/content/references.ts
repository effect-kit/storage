import { Effect, Schema } from "effect";
import type { SecureRandom } from "effect/crypto";

import { digestText } from "../runtime/portable-hash.js";
import type { StorageService } from "../storage.js";
import { workflowError } from "../workflows/store.js";
import type { RecordStore } from "../workflows/store.js";
import {
  ContentId,
  ContentRecord,
  ReferenceEdge,
  ReferenceIntent,
  ReferenceName,
  ReferencePointer,
} from "./schema.js";

export const contentKey = (id: string) => `content:${id}`;
const pointerKey = (reference: string) => `reference:${digestText(reference)}`;
const intentKey = (reference: string) => `intent:${digestText(reference)}`;
export const makeReferences = (
  store: RecordStore,
  binding: string,
  storage: StorageService,
  random: SecureRandom.SecureRandom["Service"],
) => {
  const load = Effect.fnUntraced(function* (id: string) {
    yield* Schema.decodeUnknownEffect(ContentId)(id).pipe(
      Effect.mapError(() => workflowError("InvalidContentId")),
    );
    const record = yield* store.read(contentKey(id), ContentRecord);
    if (!record || record.binding !== binding) return yield* workflowError("ContentNotFound");
    return record;
  });
  const reconcile = Effect.fnUntraced(function* (reference: string) {
    const intent = yield* store.read(intentKey(reference), ReferenceIntent);
    if (!intent || intent.state === "completed") return intent;
    const pointer = yield* store.read(pointerKey(reference), ReferencePointer);
    if (pointer?.intentId !== intent.id) return intent;
    if (intent.contentId && intent.edge !== undefined)
      yield* store.write(`${contentKey(intent.contentId)}:edge:${intent.edge}`, ReferenceEdge, {
        reference,
        intentId: intent.id,
        state: "committed",
      });
    const completed: ReferenceIntent = { ...intent, state: "completed" };
    yield* store.write(intentKey(reference), ReferenceIntent, completed);
    return completed;
  });
  const update = Effect.fnUntraced(function* (
    reference: string,
    contentId?: string,
    replace = false,
  ) {
    yield* Schema.decodeUnknownEffect(ReferenceName)(reference).pipe(
      Effect.mapError(() => workflowError("InvalidReference")),
    );
    const pending = yield* reconcile(reference);
    if (pending?.state === "pending") return yield* workflowError("ReferenceOutcomeUnknown");
    const previous = yield* store.read(pointerKey(reference), ReferencePointer);
    if (previous?.contentId === contentId && previous) return previous;
    if (contentId && previous?.contentId && !replace)
      return yield* workflowError("ReferenceConflict");
    const intentId = yield* random.uuidV4();
    let edge: number | undefined;
    if (contentId) {
      const record = yield* load(contentId);
      if (record.state !== "ready" || !record.object)
        return yield* workflowError("ContentNotReady");
      const head = yield* storage.head({ ref: record.object.ref });
      if (head.etag !== record.object.etag || head.size !== record.size)
        return yield* workflowError("ContentChanged");
      edge = record.edgeCount;
      // Reserve and publish the edge before the pointer. Prepared edges conservatively block GC.
      yield* store.write(contentKey(contentId), ContentRecord, { ...record, edgeCount: edge + 1 });
      yield* store.write(`${contentKey(contentId)}:edge:${edge}`, ReferenceEdge, {
        reference,
        intentId,
        state: "prepared",
      });
    }
    const intent: ReferenceIntent = {
      id: intentId,
      reference,
      state: "pending",
      ...(contentId ? { contentId } : {}),
      ...(edge !== undefined ? { edge } : {}),
    };
    yield* store.write(intentKey(reference), ReferenceIntent, intent);
    const pointer = { reference, intentId, ...(contentId ? { contentId } : {}) };
    // A failed/unknown pointer write is never blindly replayed; reconcile observes this unique intent.
    yield* store.write(pointerKey(reference), ReferencePointer, pointer);
    yield* reconcile(reference);
    return pointer;
  });
  const reachable = Effect.fnUntraced(function* (record: ContentRecord) {
    for (let index = 0; index < record.edgeCount; index++) {
      const edge = yield* store.read(`${contentKey(record.id)}:edge:${index}`, ReferenceEdge);
      if (!edge) return true;
      if (edge.state === "prepared") return true;
      const pointer = yield* store.read(pointerKey(edge.reference), ReferencePointer);
      if (pointer?.contentId === record.id) return true;
    }
    return false;
  });
  const inspect = Effect.fnUntraced(function* (
    id: string,
    options: { readonly offset?: number; readonly limit?: number } = {},
  ) {
    const record = yield* load(id);
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
    const references: string[] = [];
    const pending: string[] = [];
    const end = Math.min(record.edgeCount, offset + limit);
    for (let index = offset; index < end; index++) {
      const edge = yield* store.read(`${contentKey(id)}:edge:${index}`, ReferenceEdge);
      if (!edge) {
        pending.push("unknown");
        continue;
      }
      if (edge.state === "prepared") pending.push(edge.reference);
      const pointer = yield* store.read(pointerKey(edge.reference), ReferencePointer);
      if (pointer?.contentId === id && !references.includes(edge.reference))
        references.push(edge.reference);
    }
    return { references, pending, ...(end < record.edgeCount ? { nextOffset: end } : {}) };
  });
  return { load, update, reconcile, reachable, inspect };
};
