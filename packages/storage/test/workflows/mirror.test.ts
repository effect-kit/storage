import { randomBytes } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Context, Effect, Layer, Redacted } from "effect";
import { KeyValueStore } from "effect/persistence";

import { storageError } from "../../src/errors.js";
import { StorageMirror, MirrorStore } from "../../src/mirror.js";
import { layer as platform, aes256Gcm } from "../../src/node.js";
import { Storage } from "../../src/storage.js";
import { MemoryStorage } from "../../src/testing.js";
import { TransferDriver } from "../../src/transfers/dependencies.js";

const bytes = (text: string) => new TextEncoder().encode(text);
const setup = Effect.fnUntraced(function* (required = true) {
  const primary = Context.get(
    yield* Layer.build(MemoryStorage.layer({ storeId: "primary" })),
    TransferDriver,
  );
  const original = Context.get(
    yield* Layer.build(MemoryStorage.layer({ storeId: "replica" })),
    TransferDriver,
  );
  const faults = { write: false, delete: false };
  const replica = {
    ...original,
    storage: {
      ...original.storage,
      put: ((request) =>
        faults.write
          ? Effect.fail(storageError("put", "test", { _tag: "TransportFailure" }, "not-started"))
          : original.storage.put(request)) as typeof original.storage.put,
      delete: (request: Parameters<typeof original.storage.delete>[0]) =>
        faults.delete
          ? Effect.fail(storageError("delete", "test", { _tag: "TransportFailure" }, "unknown"))
          : original.storage.delete(request),
    },
  };
  const protection = aes256Gcm(Redacted.make(randomBytes(32)));
  const make = Effect.fnUntraced(function* () {
    const store = yield* MirrorStore.make({
      namespace: "test",
      ownership: "exclusive",
      protection,
    });
    return yield* StorageMirror.make({
      configurationId: "test",
      primary,
      replicas: [{ storage: replica, required }],
      maxBytes: 1024 * 1024,
      transfer: { retryBaseMillis: 1, retryMaxMillis: 2, maxAttempts: 8 },
    }).pipe(Effect.provideService(MirrorStore, store));
  });
  return {
    primary: primary.storage,
    replica: original.storage,
    faults,
    make,
    mirror: yield* make(),
  };
});
const text = (storage: Storage["Service"], key: string) =>
  Storage.getText({ key, maxBytes: 1024 }).pipe(Effect.provideService(Storage, storage));

it.live("mirrors expose ordinary Storage with logical identities and conditional updates", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const first = yield* test.mirror.storage.put({ key: "document", body: bytes("first") });
      assert.strictEqual(yield* text(test.mirror.storage, "document"), "first");
      assert.strictEqual(yield* text(test.replica, "document"), "first");
      assert.notStrictEqual(first.ref.storeId, test.primary.storeId);
      assert.isUndefined(first.customMetadata?.["effect-storage-mirror"]);
      const second = yield* test.mirror.storage.put({
        key: "document",
        body: bytes("second"),
        condition: { _tag: "IfMatch", etag: first.etag ?? "" },
      });
      assert.notStrictEqual(second.etag, first.etag);
      assert.strictEqual(yield* text(test.replica, "document"), "second");
      assert.strictEqual((yield* test.mirror.storage.listPage()).entries.length, 1);
      assert.strictEqual(
        (yield* Effect.result(test.mirror.storage.head({ ref: first.ref })))._tag,
        "Failure",
      );
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("required replica failures remain unpublished until durable repair succeeds", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      test.faults.write = true;
      const receipt = yield* test.mirror.write({
        id: "required",
        key: "document",
        body: bytes("content"),
      });
      assert.strictEqual(receipt.operation.state, "partial");
      assert.strictEqual(
        (yield* Effect.result(test.mirror.storage.head({ key: "document" })))._tag,
        "Failure",
      );
      test.faults.write = false;
      const restarted = yield* test.make();
      const plan = yield* restarted.planRepair({ prefix: "doc" });
      assert.strictEqual((yield* restarted.applyRepair(plan.id)).state, "completed");
      assert.strictEqual((yield* restarted.status(receipt.operation.id)).state, "completed");
      assert.strictEqual(yield* text(restarted.storage, "document"), "content");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("verified replica fallback can restore a missing primary", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      yield* test.mirror.storage.put({ key: "document", body: bytes("content") });
      yield* test.primary.delete({ key: "document" });
      assert.strictEqual(yield* text(test.mirror.storage, "document"), "content");
      const plan = yield* test.mirror.planRepair({ prefix: "document" });
      yield* test.mirror.applyRepair(plan.id);
      assert.strictEqual(yield* text(test.primary, "document"), "content");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("tombstones prevent resurrection through stale repair plans and replica fallback", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      yield* test.mirror.storage.put({ key: "document", body: bytes("content") });
      const stale = yield* test.mirror.planRepair({ prefix: "document" });
      test.faults.delete = true;
      const deleted = yield* test.mirror.remove({ key: "document" });
      assert.strictEqual(deleted.state, "partial");
      assert.strictEqual(
        (yield* Effect.result(test.mirror.storage.get({ key: "document" })))._tag,
        "Failure",
      );
      yield* test.mirror.applyRepair(stale.id);
      assert.strictEqual((yield* test.primary.listPage()).entries.length, 0);
      test.faults.delete = false;
      yield* test.mirror.storage.delete({ key: "document" });
      assert.strictEqual((yield* test.replica.listPage()).entries.length, 0);
      assert.strictEqual((yield* test.mirror.storage.listPage()).entries.length, 0);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("best-effort failures remain visible and foreign replica bytes are never overwritten", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup(false);
      yield* test.replica.put({ key: "document", body: bytes("foreign") });
      yield* test.mirror.storage.put({ key: "document", body: bytes("owned") });
      assert.strictEqual((yield* test.mirror.statusFor("document"))?.state, "partial");
      assert.strictEqual(yield* text(test.mirror.storage, "document"), "owned");
      assert.strictEqual(yield* text(test.replica, "document"), "foreign");
      const plan = yield* test.mirror.planRepair({ prefix: "document" });
      yield* test.mirror.applyRepair(plan.id);
      assert.strictEqual(yield* text(test.replica, "document"), "foreign");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);
