import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { Clock, Context, Effect, Layer, Redacted, Schema } from "effect";
import { KeyValueStore } from "effect/persistence";

import { generateX25519Identity, identityToRecipient } from "age-encryption";

import { StoragePrefix } from "../../dist/composition.js";
import { StorageCompression } from "../../dist/compression.js";
import { ContentStore, ContentReferenceStore } from "../../dist/content.js";
import { StorageEncryption, StorageEncryptionCodec } from "../../dist/encryption.js";
import { AgeEncryption } from "../../dist/encryption/age.js";
import { FilesystemStorage } from "../../dist/filesystem.js";
import { Storage } from "../../dist/index.js";
import { StorageMigrations, MigrationStore } from "../../dist/migrations.js";
import { StorageMirror, MirrorStore } from "../../dist/mirror.js";
import { aes256Gcm, atomicFileStore, layer as platform } from "../../dist/node.js";
import { S3Storage } from "../../dist/s3.js";
import { TransferDriver, UploadSource } from "../../dist/transfers.js";

const [filename, scenario, phase] = process.argv.slice(2);
const config = JSON.parse(await readFile(filename, "utf8"));
assert.equal(config.mode, "owned-local");
assert.equal(new URL(config.endpoint).hostname, "127.0.0.1");
assert(config.bucket.startsWith("composition-"));
const payload = new Uint8Array(512 * 1024 + 13).fill(37);
const kill = Effect.sync(() => {
  process.kill(process.pid, "SIGKILL");
}).pipe(Effect.andThen(Effect.never));
const endpoint = (provider) =>
  Effect.map(Layer.build(provider), (context) => Context.get(context, TransferDriver));
const takeBytes = (storage, key) =>
  Storage.getBytes({ key, maxBytes: 2 * 1024 ** 2 }).pipe(Effect.provideService(Storage, storage));

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const s3Options = {
        bucket: config.bucket,
        endpoint: config.endpoint,
        region: "us-east-1",
        forcePathStyle: true,
        credentials: config.credentials,
      };
      const rawS3 = S3Storage.layer(s3Options);
      const remote = yield* endpoint(
        StoragePrefix.layer({ prefix: `${scenario}/` }).pipe(Layer.provide(rawS3)),
      );
      const local = yield* endpoint(
        FilesystemStorage.layer({
          root: join(config.root, scenario, "objects"),
          storeId: `filesystem:${scenario}`,
        }),
      );
      const kvs = yield* atomicFileStore(join(config.root, "journal"));
      const protection = aes256Gcm(Redacted.make(Buffer.from(config.protectionKey, "base64")));
      const persisted = (effect) =>
        effect.pipe(Effect.provideService(KeyValueStore.KeyValueStore, kvs));
      const journalOptions = { namespace: scenario, ownership: "exclusive", protection };
      const migrationStore = yield* persisted(MigrationStore.make(journalOptions));
      const migration = (source, destination, configurationId = scenario) =>
        StorageMigrations.make({ source, destination, configurationId }).pipe(
          Effect.provideService(MigrationStore, migrationStore),
        );

      if (scenario === "baseline") {
        yield* local.storage.put({ key: "docs/a", body: payload });
        const forward = yield* migration(local, remote, "forward");
        const planned = yield* forward.plan({
          id: "forward",
          prefix: "docs/",
          conflict: "fail",
          deleteSource: false,
        });
        assert.equal((yield* forward.apply(planned.id)).state, "completed");
        assert.deepEqual(yield* takeBytes(remote.storage, "docs/a"), payload);
        const reverse = yield* migration(remote, local, "reverse");
        yield* reverse.plan({
          id: "reverse",
          prefix: "docs/",
          destinationPrefix: "restored/",
          conflict: "fail",
          deleteSource: false,
        });
        assert.equal((yield* reverse.apply("reverse")).state, "completed");
        assert.deepEqual(yield* takeBytes(local.storage, "restored/a"), payload);
        const nativeSource = yield* endpoint(rawS3);
        const nativeDestination = yield* endpoint(
          S3Storage.layer({ ...s3Options, bucket: config.otherBucket }),
        );
        const native = yield* migration(nativeSource, nativeDestination, "native-copy");
        yield* native.plan({
          id: "native-copy",
          prefix: "baseline/docs/",
          conflict: "fail",
          deleteSource: false,
        });
        assert.equal((yield* native.apply("native-copy")).state, "completed");
        assert.equal((yield* native.inspect("native-copy")).entries[0].strategy, "native-copy");
        const limits = { maxDecodedBytes: 2 * 1024 ** 2, maxStoredBytes: 3 * 1024 ** 2 };
        const compressed = yield* StorageCompression.make({
          ...limits,
          format: "gzip",
          maxExpansionRatio: 10000,
        }).pipe(Effect.provideService(Storage, remote.storage));
        yield* compressed.put({
          key: "compressed",
          body: payload,
          contentType: "application/octet-stream",
        });
        assert.deepEqual(yield* takeBytes(compressed, "compressed"), payload);
        const identity = yield* Effect.promise(() => generateX25519Identity());
        const recipient = yield* Effect.promise(() => identityToRecipient(identity));
        const codec = yield* AgeEncryption.make({
          recipients: [recipient],
          identities: Effect.succeed([Redacted.make(identity)]),
        });
        const encrypted = yield* StorageEncryption.make(limits).pipe(
          Effect.provideService(Storage, remote.storage),
          Effect.provideService(StorageEncryptionCodec, codec),
        );
        yield* encrypted.put({
          key: "encrypted",
          body: payload,
          customMetadata: { confidential: "not-public" },
        });
        assert.deepEqual(yield* takeBytes(encrypted, "encrypted"), payload);
        assert(
          !JSON.stringify(yield* remote.storage.head({ key: "encrypted" })).includes("not-public"),
        );
        const mirrors = yield* persisted(MirrorStore.make(journalOptions));
        const mirror = yield* StorageMirror.make({
          configurationId: "baseline",
          primary: local,
          replicas: [{ storage: remote, required: true }],
          maxBytes: 2 * 1024 ** 2,
        }).pipe(Effect.provideService(MirrorStore, mirrors));
        yield* mirror.storage.put({ key: "mirrored", body: payload });
        assert.deepEqual(yield* takeBytes(mirror.storage, "mirrored"), payload);
        const deletion = yield* mirror.remove({ key: "mirrored" });
        assert.equal(deletion.state, "partial");
        assert.equal(deletion.outcomes[0].error.reason._tag, "Unsupported");
        assert.equal(
          (yield* Effect.result(mirror.storage.head({ key: "mirrored" })))._tag,
          "Failure",
        );
        const references = yield* persisted(ContentReferenceStore.make(journalOptions));
        const content = yield* ContentStore.make({
          configurationId: "baseline",
          namespace: "native",
          maxBytes: 2 * 1024 ** 2,
          minimumRetentionMillis: 1,
        }).pipe(
          Effect.provideService(ContentReferenceStore, references),
          Effect.provideService(Storage, remote.storage),
          Effect.provideService(TransferDriver, remote),
        );
        const stored = yield* content.put({
          source: UploadSource.bytes(payload),
          reference: "document",
        });
        const duplicate = yield* content.put({ source: UploadSource.bytes(payload) });
        assert.equal(stored.id, duplicate.id);
        yield* content.detach({ reference: "document" });
        yield* Effect.sleep("5 millis");
        const garbage = yield* content.planGarbageCollection({
          olderThan: yield* Clock.currentTimeMillis,
        });
        assert.equal((yield* content.applyGarbageCollection(garbage.id)).state, "completed");
        assert.equal((yield* content.status(stored.id)).state, "deleted");
        return;
      }
      if (scenario.startsWith("migration-")) {
        const destination =
          phase === "crash"
            ? {
                ...local,
                storage: {
                  ...local.storage,
                  put: (request) =>
                    scenario === "migration-before-native"
                      ? kill
                      : local.storage.put(request).pipe(Effect.andThen(kill)),
                },
              }
            : local;
        const migrations = yield* migration(remote, destination);
        if (phase === "crash") {
          yield* remote.storage.put({ key: "docs/a", body: payload });
          yield* migrations.plan({
            id: "crash",
            prefix: "docs/",
            conflict: "fail",
            deleteSource: true,
          });
          yield* migrations.apply("crash");
          throw new Error("Missing migration crash boundary");
        }
        const recovered = yield* migrations.reconcile("crash");
        if (scenario === "migration-after-native") {
          assert.equal(recovered.state, "completed");
          assert.equal((yield* remote.storage.listPage()).entries.length, 0);
          assert.deepEqual(yield* takeBytes(local.storage, "docs/a"), payload);
        } else {
          assert.equal(recovered.state, "partial");
          assert.equal((yield* migrations.inspect("crash")).entries[0].state, "unknown");
          assert.deepEqual(yield* takeBytes(remote.storage, "docs/a"), payload);
        }
        return;
      }
      if (scenario.startsWith("mirror-")) {
        const journal = yield* persisted(MirrorStore.make(journalOptions));
        const primaryBackend = scenario === "mirror-delete-after-primary" ? remote : local;
        const replicaBackend =
          scenario === "mirror-delete-after-primary"
            ? yield* endpoint(
                StoragePrefix.layer({ prefix: `${scenario}/` }).pipe(
                  Layer.provide(S3Storage.layer({ ...s3Options, bucket: config.otherBucket })),
                ),
              )
            : remote;
        const primary = {
          ...primaryBackend,
          storage: {
            ...primaryBackend.storage,
            put: (request) =>
              primaryBackend.storage
                .put(request)
                .pipe(
                  Effect.flatMap((receipt) =>
                    phase === "crash" && scenario === "mirror-after-primary"
                      ? kill
                      : Effect.succeed(receipt),
                  ),
                ),
            delete: (request) =>
              primaryBackend.storage
                .delete(request)
                .pipe(
                  Effect.andThen(
                    phase === "crash" && scenario === "mirror-delete-after-primary"
                      ? kill
                      : Effect.void,
                  ),
                ),
          },
        };
        const mirror = yield* StorageMirror.make({
          configurationId: scenario,
          primary,
          replicas: [{ storage: replicaBackend, required: true }],
          maxBytes: 2 * 1024 ** 2,
        }).pipe(Effect.provideService(MirrorStore, journal));
        if (phase === "crash") {
          yield* mirror.write({ id: "crash-write", key: "document", body: payload });
          if (scenario === "mirror-delete-after-primary")
            yield* mirror.storage.delete({ key: "document" });
          throw new Error("Missing mirror crash boundary");
        }
        if (scenario === "mirror-delete-after-primary")
          assert.equal(
            (yield* Effect.result(mirror.storage.head({ key: "document" })))._tag,
            "Failure",
          );
        const repair = yield* mirror.planRepair({ prefix: "document" });
        assert.equal((yield* mirror.applyRepair(repair.id)).state, "completed");
        if (scenario === "mirror-after-primary")
          assert.deepEqual(yield* takeBytes(mirror.storage, "document"), payload);
        else
          assert.equal(
            (yield* Effect.result(mirror.storage.head({ key: "document" })))._tag,
            "Failure",
          );
        return;
      }
      const originalJournal = yield* persisted(ContentReferenceStore.make(journalOptions));
      const journal = {
        ...originalJournal,
        write: (id, schema, value) => {
          if (phase === "crash" && id.startsWith("reference:")) {
            if (scenario === "content-before-pointer") return kill;
            if (scenario === "content-after-pointer")
              return originalJournal.write(id, schema, value).pipe(Effect.andThen(kill));
          }
          return originalJournal.write(id, schema, value);
        },
      };
      const backend = {
        ...remote.storage,
        delete: (request) =>
          remote.storage
            .delete(request)
            .pipe(
              Effect.andThen(
                phase === "crash" && scenario === "gc-after-native-delete" ? kill : Effect.void,
              ),
            ),
      };
      const content = yield* ContentStore.make({
        configurationId: scenario,
        namespace: "native",
        maxBytes: 2 * 1024 ** 2,
        minimumRetentionMillis: 1,
      }).pipe(
        Effect.provideService(ContentReferenceStore, journal),
        Effect.provideService(Storage, backend),
        Effect.provideService(TransferDriver, { ...remote, storage: backend }),
      );
      if (phase === "crash") {
        const stored = yield* content.put({ source: UploadSource.bytes(payload) });
        yield* originalJournal.write("fixture-content-id", Schema.String, stored.id);
        if (scenario.startsWith("content-"))
          yield* content.attach({ contentId: stored.id, reference: "document" });
        else {
          yield* Effect.sleep("5 millis");
          const garbage = yield* content.planGarbageCollection({
            olderThan: yield* Clock.currentTimeMillis,
          });
          yield* originalJournal.write("fixture-plan-id", Schema.String, garbage.id);
          yield* content.applyGarbageCollection(garbage.id);
        }
        throw new Error("Missing content crash boundary");
      }
      const id = yield* originalJournal.read("fixture-content-id", Schema.String);
      assert(id);
      if (scenario === "gc-after-native-delete") {
        const planId = yield* originalJournal.read("fixture-plan-id", Schema.String);
        assert.equal((yield* content.applyGarbageCollection(planId)).state, "completed");
        assert.equal((yield* content.status(id)).state, "deleted");
      } else {
        assert.deepEqual((yield* content.references(id)).pending, ["document"]);
        assert.equal(
          (yield* content.planGarbageCollection({ olderThan: yield* Clock.currentTimeMillis }))
            .count,
          0,
        );
        const reconciled = yield* content.reconcileReference("document");
        assert.equal(
          reconciled.state,
          scenario === "content-after-pointer" ? "completed" : "pending",
        );
      }
    }),
  ).pipe(Effect.provide(platform)),
);
process.stdout.write(`Passed ${scenario}/${phase}\n`);
