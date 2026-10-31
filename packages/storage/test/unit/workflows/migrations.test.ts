import { randomBytes } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Context, Effect, Layer, Redacted, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { StoragePrefix } from "../../../src/composition/views.js";
import { storageError } from "../../../src/core/errors.js";
import { layer as platform, aes256Gcm } from "../../../src/runtime/node.js";
import { MemoryStorage } from "../../../src/testing/index.js";
import { TransferDriver } from "../../../src/transfers/dependencies.js";
import { StorageMigrations, MigrationStore } from "../../../src/workflows/migrations/index.js";
import { StorageMirror, MirrorStore } from "../../../src/workflows/mirror/index.js";

const bytes = (text: string) => new TextEncoder().encode(text);

it.live("workflows reject overlapping replicas through composed mirror layers", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const options = {
        namespace: "overlap",
        ownership: "exclusive" as const,
        protection: aes256Gcm(Redacted.make(randomBytes(32))),
      };
      const replicaLive = MemoryStorage.layer({ storeId: "shared-replica" });
      const mirrorStore = yield* MirrorStore.make(options);
      const mirrorLive = StorageMirror.layer({
        configurationId: "overlap",
        primary: MemoryStorage.layer({ storeId: "primary" }),
        replicas: [{ storage: replicaLive, required: true }],
        maxBytes: 1024,
      }).pipe(Layer.provide(Layer.succeed(MirrorStore, mirrorStore)));
      const facade = Context.get(
        yield* Layer.build(
          StoragePrefix.layer({ prefix: "tenant/" }).pipe(Layer.provide(mirrorLive)),
        ),
        TransferDriver,
      );
      const replica = Context.get(yield* Layer.build(replicaLive), TransferDriver);
      const migrationStore = yield* MigrationStore.make(options);
      const migration = yield* Effect.result(
        StorageMigrations.make({
          configurationId: "overlap",
          source: facade,
          destination: replica,
        }).pipe(Effect.provideService(MigrationStore, migrationStore)),
      );
      assert.strictEqual(migration._tag, "Failure");
      const nestedMirror = yield* Effect.result(
        StorageMirror.make({
          configurationId: "nested",
          primary: facade,
          replicas: [{ storage: replica, required: true }],
          maxBytes: 1024,
        }).pipe(Effect.provideService(MirrorStore, mirrorStore)),
      );
      assert.strictEqual(nestedMirror._tag, "Failure");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

const setup = Effect.fnUntraced(function* (lost = false) {
  const source = Context.get(
    yield* Layer.build(MemoryStorage.layer({ storeId: "source" })),
    TransferDriver,
  );
  const original = Context.get(
    yield* Layer.build(MemoryStorage.layer({ storeId: "destination" })),
    TransferDriver,
  );
  let shouldLose = lost;
  const destination = {
    ...original,
    storage: {
      ...original.storage,
      put: ((request) =>
        original.storage.put(request).pipe(
          Effect.flatMap((receipt) => {
            if (!shouldLose) return Effect.succeed(receipt);
            shouldLose = false;
            return Effect.fail(
              storageError("put", "test", { _tag: "TransportFailure" }, "unknown"),
            );
          }),
        )) as typeof original.storage.put,
    },
  };
  const protection = aes256Gcm(Redacted.make(randomBytes(32)));
  const make = Effect.fnUntraced(function* (configurationId = "test") {
    const store = yield* MigrationStore.make({
      namespace: "test",
      ownership: "exclusive",
      protection,
    });
    return yield* StorageMigrations.make({
      configurationId,
      source,
      destination,
      transfer: { maxAttempts: 64, retryBaseMillis: 1, retryMaxMillis: 2 },
    }).pipe(Effect.provideService(MigrationStore, store));
  });
  return {
    source: source.storage,
    destination: destination.storage,
    make,
    migrations: yield* make(),
  };
});

it.live(
  "migration manifests are inspectable dry runs and bounded batches survive reconstruction",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const test = yield* setup();
        yield* test.source.put({ key: "docs/a", body: bytes("a") });
        yield* test.source.put({ key: "docs/b", body: bytes("b") });
        const plan = yield* test.migrations.plan({
          id: "dry-run",
          prefix: "docs/",
          destinationPrefix: "new/",
          conflict: "fail",
          deleteSource: false,
        });
        assert.strictEqual(plan.count, 2);
        assert.strictEqual((yield* test.destination.listPage()).entries.length, 0);
        assert.strictEqual((yield* test.migrations.inspect(plan.id, { limit: 1 })).nextOffset, 1);
        assert.strictEqual((yield* test.migrations.apply(plan.id, { limit: 1 })).state, "applying");
        const restarted = yield* test.make();
        assert.strictEqual((yield* restarted.resume(plan.id)).state, "completed");
        assert.strictEqual((yield* test.destination.head({ key: "new/a" })).size, 1);
        assert.strictEqual((yield* test.source.listPage()).entries.length, 2);
        const foreign = yield* test.make("foreign-configuration");
        assert.strictEqual((yield* Effect.result(foreign.status(plan.id)))._tag, "Failure");
      }),
    ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("lost copy responses reconcile owned bytes before source deletion", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup(true);
      yield* test.source.put({ key: "move/a", body: bytes("owned") });
      const plan = yield* test.migrations.plan({
        id: "lost",
        prefix: "move/",
        conflict: "fail",
        deleteSource: true,
      });
      assert.strictEqual((yield* test.migrations.apply(plan.id)).state, "partial");
      assert.strictEqual((yield* test.source.head({ key: "move/a" })).size, 5);
      assert.strictEqual((yield* (yield* test.make()).reconcile(plan.id)).state, "completed");
      assert.strictEqual((yield* test.source.listPage()).entries.length, 0);
      assert.strictEqual((yield* test.destination.head({ key: "move/a" })).size, 5);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("concurrent source/destination changes cannot be silently overwritten or adopted", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      yield* test.source.put({ key: "docs/a", body: bytes("old") });
      const plan = yield* test.migrations.plan({
        id: "changed",
        prefix: "docs/",
        conflict: "fail",
        deleteSource: true,
      });
      yield* test.destination.put({ key: "docs/a", body: bytes("old") });
      assert.strictEqual((yield* test.migrations.apply(plan.id)).state, "partial");
      assert.strictEqual((yield* test.migrations.reconcile(plan.id)).state, "partial");
      assert.strictEqual((yield* test.source.head({ key: "docs/a" })).size, 3);
      const next = yield* test.migrations.plan({
        id: "source-changed",
        prefix: "docs/",
        destinationPrefix: "next/",
        conflict: "fail",
        deleteSource: false,
      });
      yield* test.source.put({ key: "docs/a", body: bytes("replacement") });
      assert.strictEqual((yield* test.migrations.apply(next.id)).state, "partial");
      assert.strictEqual((yield* test.destination.listPage({ prefix: "next/" })).entries.length, 0);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live(
  "sync deletion is explicitly planned and rechecks source absence and destination identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const test = yield* setup();
        yield* test.source.put({ key: "docs/retained", body: bytes("live") });
        yield* test.destination.put({ key: "docs/extra", body: bytes("extra") });
        const plan = yield* test.migrations.plan({
          id: "sync",
          prefix: "docs/",
          conflict: "skip",
          deleteSource: false,
          deleteExtraneous: true,
        });
        assert.strictEqual(plan.count, 2);
        const rows = (yield* test.migrations.inspect(plan.id)).entries;
        assert.isTrue(rows.some((entry) => entry.action === "delete"));
        yield* test.source.put({ key: "docs/extra", body: bytes("new-source") });
        assert.strictEqual((yield* test.migrations.apply(plan.id)).state, "partial");
        assert.strictEqual((yield* test.destination.head({ key: "docs/extra" })).size, 5);
      }),
    ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);

it.live("explicit pinned inventories select stable entries and preserve producer failures", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* setup();
      const selected = yield* test.source.put({ key: "docs/selected", body: bytes("selected") });
      yield* test.source.put({ key: "docs/excluded", body: bytes("excluded") });
      const plan = yield* test.migrations.plan({
        id: "inventory",
        prefix: "docs/",
        conflict: "fail",
        deleteSource: false,
        inventoryId: "snapshot-1",
        inventory: Stream.succeed(selected),
      });
      assert.strictEqual(plan.count, 1);
      assert.strictEqual((yield* test.migrations.apply(plan.id)).state, "completed");
      assert.strictEqual((yield* test.destination.listPage()).entries.length, 1);
      const failed = yield* Effect.result(
        test.migrations.plan({
          prefix: "docs/",
          conflict: "fail",
          deleteSource: false,
          inventoryId: "broken",
          inventory: Stream.fail("inventory-failed" as const),
        }),
      );
      assert.strictEqual(failed._tag, "Failure");
      if (failed._tag === "Failure") assert.strictEqual(failed.failure, "inventory-failed");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory), Effect.provide(platform)),
);
