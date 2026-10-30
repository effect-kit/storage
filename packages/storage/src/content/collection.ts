import { Clock, Effect, Schema } from "effect";
import type { SecureRandom } from "effect/crypto";

import type { StorageService } from "../storage.js";
import { deletePinned } from "../workflows/objects.js";
import { workflowError } from "../workflows/store.js";
import type { RecordStore } from "../workflows/store.js";
import type { makeReferences } from "./references.js";
import { contentKey } from "./references.js";
import { CollectionEntry, CollectionPlan, ContentRecord } from "./schema.js";

export const makeCollection = (
  store: RecordStore,
  binding: string,
  storage: StorageService,
  references: ReturnType<typeof makeReferences>,
  random: SecureRandom.SecureRandom["Service"],
  minimumRetentionMillis: number,
) => {
  const plan = Effect.fnUntraced(function* (input: { readonly olderThan: number }) {
    if (
      storage.capabilities.versionedReads === "none" &&
      !storage.capabilities.deleteConditions.includes("if-match")
    )
      return yield* workflowError("ConditionalCollectionUnsupported");
    if (!Number.isFinite(input.olderThan)) return yield* workflowError("InvalidCutoff");
    const cutoff = Math.min(
      input.olderThan,
      (yield* Clock.currentTimeMillis) - minimumRetentionMillis,
    );
    const id = yield* random.uuidV4();
    const upper = (yield* store.read("content-sequence", Schema.Natural)) ?? 0;
    let count = 0;
    for (let index = 1; index <= upper; index++) {
      const contentId = yield* store.read(`content-index:${index}`, Schema.String);
      if (!contentId || (yield* store.read(`gc:${id}:seen:${contentId}`, Schema.Boolean))) continue;
      const record = yield* store.read(contentKey(contentId), ContentRecord);
      if (!record) continue;
      if (record.binding !== binding) return yield* workflowError("ForeignContentIndex");
      if (
        record.state !== "ready" ||
        record.createdAt >= cutoff ||
        record.retainUntil > (yield* Clock.currentTimeMillis) ||
        (yield* references.reachable(record))
      )
        continue;
      yield* store.write(`gc:${id}:entry:${count++}`, CollectionEntry, {
        contentId,
        generation: record.generation,
        state: "planned",
      });
      yield* store.write(`gc:${id}:seen:${contentId}`, Schema.Boolean, true);
    }
    const result: CollectionPlan = { id, binding, count, cutoff, state: "planned" };
    yield* store.write(`gc:${id}`, CollectionPlan, result);
    return result;
  });
  const inspect = Effect.fnUntraced(function* (
    id: string,
    options: { readonly offset?: number; readonly limit?: number } = {},
  ) {
    const header = yield* store.read(`gc:${id}`, CollectionPlan);
    if (!header || header.binding !== binding)
      return yield* workflowError("CollectionPlanNotFound");
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
    const entries = [];
    for (let index = offset; index < Math.min(header.count, offset + limit); index++)
      entries.push(yield* store.read(`gc:${id}:entry:${index}`, CollectionEntry));
    return {
      plan: header,
      entries,
      ...(offset + entries.length < header.count ? { nextOffset: offset + entries.length } : {}),
    };
  });
  const apply = Effect.fnUntraced(function* (
    id: string,
    options: { readonly limit?: number } = {},
  ) {
    let header = yield* store.read(`gc:${id}`, CollectionPlan);
    if (!header || header.binding !== binding)
      return yield* workflowError("CollectionPlanNotFound");
    const limit = options.limit ?? 100;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000)
      return yield* workflowError("InvalidBatchLimit");
    let attempted = 0;
    let partial = false;
    let remaining = false;
    for (let index = 0; index < header.count; index++) {
      const key = `gc:${id}:entry:${index}`;
      const entry = yield* store.read(key, CollectionEntry);
      if (!entry) return yield* workflowError("MissingCollectionEntry");
      if (["collected", "retained"].includes(entry.state)) continue;
      if (attempted++ >= limit) {
        remaining = true;
        continue;
      }
      let record = yield* references.load(entry.contentId);
      if (record.generation === entry.generation && record.state === "deleted") {
        yield* store.write(key, CollectionEntry, { ...entry, state: "collected" });
        continue;
      }
      if (
        record.generation !== entry.generation ||
        record.state === "deleted" ||
        record.createdAt >= header.cutoff ||
        record.retainUntil > (yield* Clock.currentTimeMillis) ||
        (yield* references.reachable(record))
      ) {
        yield* store.write(key, CollectionEntry, { ...entry, state: "retained" });
        continue;
      }
      const object = record.object;
      if (!object) return yield* workflowError("MissingContentIdentity");
      record = { ...record, state: "deleting" };
      yield* store.write(contentKey(record.id), ContentRecord, record);
      const removed = yield* Effect.result(deletePinned(storage, object));
      if (removed._tag === "Failure") {
        yield* store.write(key, CollectionEntry, { ...entry, state: "unknown" });
        partial = true;
        continue;
      }
      yield* store.write(contentKey(record.id), ContentRecord, { ...record, state: "deleted" });
      yield* store.write(key, CollectionEntry, { ...entry, state: "collected" });
    }
    header = { ...header, state: remaining ? "applying" : partial ? "partial" : "completed" };
    yield* store.write(`gc:${id}`, CollectionPlan, header);
    return header;
  });
  return { plan, inspect, apply };
};
