// Run after pnpm build. Replace memory/KVS and the ephemeral key with application services.
import { randomBytes } from "node:crypto";

import { Context, Effect, Layer, Redacted } from "effect";
import { KeyValueStore } from "effect/persistence";

import { ContentStore, ContentReferenceStore } from "../dist/content.js";
import { Storage } from "../dist/index.js";
import { StorageMigrations, MigrationStore } from "../dist/migrations.js";
import { StorageMirror, MirrorStore } from "../dist/mirror.js";
import { aes256Gcm, layer as platform } from "../dist/node.js";
import { MemoryStorage } from "../dist/testing.js";
import { UploadSource } from "../dist/transfers.js";

const protection = aes256Gcm(Redacted.make(randomBytes(32)));
const journalOptions = { namespace: "example", ownership: "exclusive", protection };
const PersistenceLive = KeyValueStore.layerMemory;

const result = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const sourceContext = yield* Layer.build(MemoryStorage.layer({ storeId: "source" }));
      const destinationContext = yield* Layer.build(
        MemoryStorage.layer({ storeId: "destination" }),
      );
      const SourceLive = Layer.succeedContext(sourceContext);
      const DestinationLive = Layer.succeedContext(destinationContext);
      yield* Context.get(sourceContext, Storage).put({
        key: "documents/a.txt",
        body: new TextEncoder().encode("migrated"),
      });

      const MigrationsLive = StorageMigrations.layer({
        configurationId: "example-migration",
        source: SourceLive,
        destination: DestinationLive,
      }).pipe(
        Layer.provide(MigrationStore.layer(journalOptions).pipe(Layer.provide(PersistenceLive))),
      );
      const migrations = Context.get(yield* Layer.build(MigrationsLive), StorageMigrations);
      const plan = yield* migrations.plan({
        prefix: "documents/",
        conflict: "fail",
        deleteSource: false,
      });
      const migration = yield* migrations.apply(plan.id);

      const MirrorLive = StorageMirror.layer({
        configurationId: "example-mirror",
        primary: SourceLive,
        replicas: [{ storage: DestinationLive, required: true }],
        maxBytes: 1024 * 1024,
      }).pipe(
        Layer.provide(MirrorStore.layer(journalOptions).pipe(Layer.provide(PersistenceLive))),
      );
      const mirror = Context.get(yield* Layer.build(MirrorLive), StorageMirror);
      const mirrored = yield* mirror.write({
        key: "mirror.txt",
        body: new TextEncoder().encode("replicated"),
      });

      const ContentLive = ContentStore.layer({
        configurationId: "example-content",
        namespace: "tenant-a",
        maxBytes: 1024 * 1024,
      }).pipe(
        Layer.provide(DestinationLive),
        Layer.provide(
          ContentReferenceStore.layer(journalOptions).pipe(Layer.provide(PersistenceLive)),
        ),
      );
      const content = Context.get(yield* Layer.build(ContentLive), ContentStore);
      const object = yield* content.put({
        source: UploadSource.bytes(new TextEncoder().encode("deduplicated")),
        reference: "document:1",
      });
      const same = yield* content.put({
        source: UploadSource.bytes(new TextEncoder().encode("deduplicated")),
        reference: "document:2",
      });
      return {
        migration: migration.state,
        mirror: mirrored.operation.state,
        deduplicated: object.id === same.id,
        references: (yield* content.references(object.id)).references,
      };
    }),
  ).pipe(Effect.provide(platform)),
);
process.stdout.write(`${JSON.stringify(result)}\n`);
