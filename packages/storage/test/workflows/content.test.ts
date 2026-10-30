import { randomBytes } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Clock, Context, Effect, Layer, Redacted, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { ContentStore, ContentReferenceStore } from "../../src/content.js";
import { storageError } from "../../src/errors.js";
import { aes256Gcm, layer as platform } from "../../src/node.js";
import { Storage } from "../../src/storage.js";
import { MemoryStorage } from "../../src/testing.js";
import { UploadSource } from "../../src/transfers.js";
import { TransferDriver } from "../../src/transfers/dependencies.js";
import { workflowError } from "../../src/workflows/store.js";

const bytes = (value: string) => new TextEncoder().encode(value);
const setup = Effect.fnUntraced(function* () {
  const original = Context.get(
    yield* Layer.build(MemoryStorage.layer({ storeId: "content" })),
    TransferDriver,
  );
  const faults = { delete: false, pointer: "none" as "none" | "before" | "after" };
  const storage = {
    ...original.storage,
    delete: (request: Parameters<typeof original.storage.delete>[0]) =>
      original.storage.delete(request).pipe(
        Effect.flatMap(() => {
          if (!faults.delete) return Effect.void;
          faults.delete = false;
          return Effect.fail(
            storageError("delete", "test", { _tag: "TransportFailure" }, "unknown"),
          );
        }),
      ),
  };
  const endpoint = { ...original, storage };
  const protection = aes256Gcm(Redacted.make(randomBytes(32)));
  const make = Effect.fnUntraced(function* () {
    const journal = yield* ContentReferenceStore.make({
      namespace: "test",
      ownership: "exclusive",
      protection,
    });
    const store: typeof journal = {
      ...journal,
      write: (key, schema, value) => {
        if (key.startsWith("reference:") && faults.pointer !== "none") {
          const mode = faults.pointer;
          faults.pointer = "none";
          const failed = Effect.fail(workflowError("InjectedUnknownPointerWrite"));
          return mode === "before"
            ? failed
            : journal.write(key, schema, value).pipe(Effect.andThen(failed));
        }
        return journal.write(key, schema, value);
      },
    };
    return yield* ContentStore.make({
      configurationId: "test",
      namespace: "tenant-a",
      maxBytes: 1024 * 1024,
      minimumRetentionMillis: 1,
    }).pipe(
      Effect.provideService(ContentReferenceStore, store),
      Effect.provideService(Storage, storage),
      Effect.provideService(TransferDriver, endpoint),
    );
  });
  return { storage, faults, make, content: yield* make() };
});
const collectable = Effect.fnUntraced(function* (content: ContentStore["Service"]) {
  yield* Effect.sleep("5 millis");
  return yield* content.planGarbageCollection({ olderThan: yield* Clock.currentTimeMillis });
});

it.live("content addressing deduplicates verified bytes and tracks independent references", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const first = yield* test.content.put({
        source: UploadSource.bytes(bytes("same")),
        reference: "document:1",
      });
      const second = yield* test.content.put({
        source: UploadSource.bytes(bytes("same")),
        reference: "document:2",
      });
      assert.strictEqual(first.id, second.id);
      assert.strictEqual((yield* test.storage.listPage()).entries.length, 1);
      assert.deepStrictEqual((yield* test.content.references(first.id)).references.toSorted(), [
        "document:1",
        "document:2",
      ]);
      assert.strictEqual((yield* collectable(test.content)).count, 0);
      yield* test.content.detach({ reference: "document:1" });
      assert.strictEqual((yield* collectable(test.content)).count, 0);
      const result = yield* test.content.get(first.id);
      if (result._tag !== "Object") return assert.fail("Missing content");
      assert.strictEqual(
        new TextDecoder().decode((yield* Stream.runCollect(result.body))[0]),
        "same",
      );
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("GC rechecks references created after planning and never collects live content", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const first = yield* test.content.put({ source: UploadSource.bytes(bytes("live")) });
      const plan = yield* collectable(test.content);
      assert.strictEqual(plan.count, 1);
      yield* test.content.attach({ contentId: first.id, reference: "new-reference" });
      assert.strictEqual((yield* test.content.applyGarbageCollection(plan.id)).state, "completed");
      assert.strictEqual((yield* test.content.status(first.id)).state, "ready");
      assert.strictEqual((yield* test.storage.listPage()).entries.length, 1);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("unknown reference publication retains bytes until uniquely observed reconciliation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const content = yield* test.content.put({ source: UploadSource.bytes(bytes("protected")) });
      test.faults.pointer = "after";
      assert.strictEqual(
        (yield* Effect.result(
          test.content.attach({ contentId: content.id, reference: "document" }),
        ))._tag,
        "Failure",
      );
      const restarted = yield* test.make();
      assert.deepStrictEqual((yield* restarted.references(content.id)).pending, ["document"]);
      assert.strictEqual((yield* collectable(restarted)).count, 0);
      assert.strictEqual((yield* restarted.reconcileReference("document"))?.state, "completed");
      yield* restarted.detach({ reference: "document" });
      assert.strictEqual((yield* collectable(restarted)).count, 1);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("unobserved pointer outcomes block reuse and GC instead of guessing non-publication", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const content = yield* test.content.put({ source: UploadSource.bytes(bytes("retained")) });
      test.faults.pointer = "before";
      yield* Effect.result(test.content.attach({ contentId: content.id, reference: "document" }));
      assert.strictEqual((yield* test.content.reconcileReference("document"))?.state, "pending");
      assert.strictEqual(
        (yield* Effect.result(test.content.detach({ reference: "document" })))._tag,
        "Failure",
      );
      assert.strictEqual((yield* collectable(test.content)).count, 0);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("GC resumes an unknown deletion and reuploads into a fresh physical generation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const first = yield* test.content.put({ source: UploadSource.bytes(bytes("collect")) });
      const plan = yield* collectable(test.content);
      test.faults.delete = true;
      assert.strictEqual((yield* test.content.applyGarbageCollection(plan.id)).state, "partial");
      assert.strictEqual((yield* test.content.status(first.id)).state, "deleting");
      assert.strictEqual(
        (yield* Effect.result(test.content.attach({ contentId: first.id, reference: "late" })))
          ._tag,
        "Failure",
      );
      const restarted = yield* test.make();
      assert.strictEqual((yield* restarted.applyGarbageCollection(plan.id)).state, "completed");
      assert.strictEqual((yield* restarted.status(first.id)).state, "deleted");
      const second = yield* restarted.put({
        source: UploadSource.bytes(bytes("collect")),
        reference: "restored",
      });
      assert.strictEqual(second.id, first.id);
      assert.notStrictEqual(second.metadata.ref.key, first.metadata.ref.key);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("a deduplication hit renews the publication grace period after a GC plan", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const initial = yield* test.content.put({ source: UploadSource.bytes(bytes("renew")) });
      const plan = yield* collectable(test.content);
      assert.strictEqual(plan.count, 1);
      yield* test.content.put({
        source: UploadSource.bytes(bytes("renew")),
        reference: "published",
      });
      yield* test.content.applyGarbageCollection(plan.id);
      assert.strictEqual((yield* test.content.status(initial.id)).state, "ready");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live(
  "concurrent attachment and collection cannot publish a reference to collected content",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const test = yield* setup();
        const content = yield* test.content.put({ source: UploadSource.bytes(bytes("race")) });
        const plan = yield* collectable(test.content);
        const [attached] = yield* Effect.all(
          [
            Effect.result(test.content.attach({ contentId: content.id, reference: "concurrent" })),
            test.content.applyGarbageCollection(plan.id),
          ],
          { concurrency: "unbounded" },
        );
        const state = yield* test.content.status(content.id);
        if (attached._tag === "Success") assert.strictEqual(state.state, "ready");
        if (state.state === "deleted")
          assert.deepStrictEqual((yield* test.content.references(content.id)).references, []);
      }),
    ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);
