import { Context, Effect, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { ObjectKey, ObjectSize, StorageSigning, Storage } from "@effect-kit/storage";
import type { ObjectMetadata, StorageError, StorageService } from "@effect-kit/storage";
import { UploadSessions } from "@effect-kit/storage/sessions";
import type { SessionsService } from "@effect-kit/storage/sessions";
import { Faults, MemoryStorage } from "@effect-kit/storage/testing";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";
import type { TransferReceipt } from "@effect-kit/storage/transfers";

const key: ObjectKey = Schema.decodeUnknownSync(ObjectKey)("a//b");
const size: ObjectSize = Schema.decodeUnknownSync(ObjectSize)(42);
// @ts-expect-error byte counts cannot be strings
const invalid: ObjectSize = "42";

class Source extends Context.Service<Source, { bytes: Uint8Array }>()("consumer/source") {}
class SourceError extends Schema.TaggedError<SourceError>()("SourceError", {}) {}
declare const adapter: StorageService;
const body = Stream.unwrap(
  Effect.map(Source, (source) =>
    Stream.concat(Stream.succeed(source.bytes), Stream.fail(new SourceError())),
  ),
);
const upload = adapter.put({ key, body });
const preserved: Effect.Effect<ObjectMetadata, StorageError | SourceError, Source> = upload;
// @ts-expect-error source environment must survive through published declarations
const missingEnvironment: Effect.Effect<ObjectMetadata, StorageError | SourceError> = upload;
// @ts-expect-error source failure must survive through published declarations
const missingError: Effect.Effect<ObjectMetadata, StorageError, Source> = upload;
const managed = Transfers.upload({ key, source: UploadSource.stream(body) });
const managedPreserved: Effect.Effect<
  TransferReceipt,
  StorageError | SourceError,
  Transfers | Source
> = managed;
// @ts-expect-error managed upload must preserve source dependencies
const managedMissingEnvironment: Effect.Effect<
  TransferReceipt,
  StorageError | SourceError,
  Transfers
> = managed;
// @ts-expect-error managed upload must preserve source failures
const managedMissingError: Effect.Effect<TransferReceipt, StorageError, Transfers | Source> =
  managed;
const downloaded = Transfers.download({
  key,
  target: { write: () => Effect.flatMap(Source, () => Effect.fail(new SourceError())) },
});
const sinkPreserved: Effect.Effect<
  TransferReceipt,
  StorageError | SourceError,
  Transfers | Source
> = downloaded;
void [managedPreserved, managedMissingEnvironment, managedMissingError, sinkPreserved];
const memory = MemoryStorage.make().pipe(Effect.flatMap((storage) => storage.put({ key, body })));
const memoryPreserved: Effect.Effect<ObjectMetadata, StorageError | SourceError, Source> = memory;
const faults = Faults.make(adapter, [{ operation: "put" }]).pipe(
  Effect.flatMap((controller) => controller.storage.put({ key, body })),
);
const faultsPreserved: Effect.Effect<ObjectMetadata, StorageError | SourceError, Source> = faults;
// @ts-expect-error get needs a scope
const unscoped: Effect.Effect<unknown, StorageError> = adapter.get({ key });
declare const download: Stream.Stream<Uint8Array, StorageError, Scope.Scope>;
// @ts-expect-error consuming an acquired response needs a scope
const unscopedBody: Effect.Effect<unknown, StorageError> = Stream.runDrain(download);
// @ts-expect-error core Storage cannot satisfy the signing service
const noSigner: Effect.Effect<unknown, never, Storage> = StorageSigning;

void [
  key,
  size,
  invalid,
  preserved,
  missingEnvironment,
  missingError,
  memoryPreserved,
  faultsPreserved,
  unscoped,
  unscopedBody,
  noSigner,
];

const durable = UploadSessions.create({ key, source: UploadSource.replayable(() => body, 10) });
const durablePreserved: Effect.Effect<string, StorageError | SourceError, UploadSessions | Source> =
  durable;
// @ts-expect-error durable source dependencies cannot disappear
const durableMissingEnvironment: Effect.Effect<string, StorageError | SourceError, UploadSessions> =
  durable;
declare const sessions: SessionsService;
const eventDelivery = sessions.deliverEvents("id", () =>
  Effect.flatMap(Source, () => Effect.fail(new SourceError())),
);
const eventPreserved: Effect.Effect<void, StorageError | SourceError, Source> = eventDelivery;
const batchDelivery = sessions.processBatch("id", () => Source);
// @ts-expect-error durable batches preserve callback environments
const batchMissingEnvironment: Effect.Effect<unknown, StorageError> = batchDelivery;
void [durablePreserved, durableMissingEnvironment, eventPreserved, batchMissingEnvironment];

import { FilesystemStorage } from "@effect-kit/storage/filesystem";
export const fileProvider = FilesystemStorage.layer({
  root: "application-files",
  storeId: "application",
});

import { R2WorkersStorage } from "@effect-kit/storage/r2-workers";
import type { R2BucketBinding } from "@effect-kit/storage/r2-workers";
declare const nativeBinding: R2BucketBinding;
export const workerProvider = R2WorkersStorage.layer({ bucket: nativeBinding, storeId: "worker" });

import { StorageEventInbox, makeEventProcessor } from "@effect-kit/storage/events";
import { StorageVersions, StorageMetadata, StorageTags } from "@effect-kit/storage/lifecycle";
import { sessionEventDelivery } from "@effect-kit/storage/session-events";
export const advancedCore = [
  StorageVersions,
  StorageMetadata,
  StorageTags,
  StorageEventInbox,
  makeEventProcessor,
  sessionEventDelivery,
] as const;

import { UploadClient, UploadClientStore } from "@effect-kit/storage/client";
import { makeStorageUploadApi, StorageHttp } from "@effect-kit/storage/http";
import type { UploadStatus } from "@effect-kit/storage/http/schema";
import { Uploads, type UploadError } from "@effect-kit/storage/uploads";
declare const file: File;
const browserLayer = UploadClient.layer({
  baseUrl: "/storage",
  headers: () => Effect.map(Source, () => ({ authorization: "Bearer fixture" })),
  store: UploadClientStore.memory(),
});
const clientProgram: Effect.Effect<UploadStatus, UploadError, UploadClient> = Effect.flatMap(
  UploadClient,
  (client) => client.upload({ file, purpose: "attachment", context: {} }),
);
const authenticatedClient: Effect.Effect<UploadStatus, UploadError, Source> = clientProgram.pipe(
  Effect.provide(browserLayer),
);
const streamContent: Effect.Effect<
  UploadStatus,
  UploadError | StorageError | SourceError,
  Uploads | Source
> = Effect.flatMap(Uploads, (uploads) =>
  uploads.content({ actor: { subject: "user" }, uploadId: "id", body }),
);
void [authenticatedClient, streamContent, makeStorageUploadApi(), StorageHttp.routes];

import { Layer, type FileSystem, type Path } from "effect";
import type { SecureRandom } from "effect/crypto";
import { KeyValueStore } from "effect/persistence";

import { StorageCache } from "@effect-kit/storage/cache";
import { StoragePrefix, StorageReadOnly } from "@effect-kit/storage/composition";
import { StorageCompression } from "@effect-kit/storage/compression";
import { ContentStore, ContentReferenceStore } from "@effect-kit/storage/content";
import { StorageEncryption, StorageEncryptionCodec } from "@effect-kit/storage/encryption";
import { StorageMigrations, MigrationStore } from "@effect-kit/storage/migrations";
import type { MigrationPlan } from "@effect-kit/storage/migrations";
import { StorageMirror, MirrorStore } from "@effect-kit/storage/mirror";
import type { MirrorOperation } from "@effect-kit/storage/mirror";
import type { CheckpointProtection } from "@effect-kit/storage/sessions";
import type { TransferHash } from "@effect-kit/storage/transfers";

declare const protection: CheckpointProtection;
declare const infrastructure: Layer.Layer<
  FileSystem.FileSystem | Path.Path | TransferHash | SecureRandom.SecureRandom
>;
const sourceLive = MemoryStorage.layer({ storeId: "source" });
const destinationLive = MemoryStorage.layer({ storeId: "destination" });
const scopedLive = StorageCache.layer({ capacity: 10, timeToLive: "1 minute" }).pipe(
  Layer.provide(StoragePrefix.layer({ prefix: "tenant/" }).pipe(Layer.provide(sourceLive))),
);
const readOnlyLive = StorageReadOnly.layer.pipe(Layer.provide(scopedLive));
const readOnlyProgram: Effect.Effect<ObjectMetadata, StorageError> = Effect.flatMap(
  Storage,
  (storage) => storage.head({ key: "x" }),
).pipe(Effect.provide(readOnlyLive));
const journals = { namespace: "packed", ownership: "exclusive" as const, protection };
const migrationsLive = StorageMigrations.layer({
  configurationId: "packed",
  source: sourceLive,
  destination: destinationLive,
}).pipe(
  Layer.provide(MigrationStore.layer(journals).pipe(Layer.provide(KeyValueStore.layerMemory))),
  Layer.provide(infrastructure),
);
const migrationProgram: Effect.Effect<MigrationPlan, StorageError> = Effect.flatMap(
  StorageMigrations,
  (migrations) => migrations.plan({ prefix: "", conflict: "fail", deleteSource: false }),
).pipe(Effect.provide(migrationsLive));
const mirrorsLive = StorageMirror.layer({
  configurationId: "packed",
  primary: sourceLive,
  replicas: [{ storage: destinationLive, required: true }],
  maxBytes: 1024,
}).pipe(
  Layer.provide(MirrorStore.layer(journals).pipe(Layer.provide(KeyValueStore.layerMemory))),
  Layer.provide(infrastructure),
);
const mirroredProgram: Effect.Effect<ObjectMetadata, StorageError | SourceError, Source> =
  Effect.flatMap(Storage, (storage) => storage.put({ key: "x", body })).pipe(
    Effect.provide(mirrorsLive),
  );
const contentLive = ContentStore.layer({
  configurationId: "packed",
  namespace: "tenant",
  maxBytes: 1024,
}).pipe(
  Layer.provide(
    ContentReferenceStore.layer(journals).pipe(Layer.provide(KeyValueStore.layerMemory)),
  ),
  Layer.provide(sourceLive),
  Layer.provide(infrastructure),
);
const contentProgram: Effect.Effect<
  { id: string; metadata: ObjectMetadata },
  StorageError | SourceError,
  Source
> = Effect.flatMap(ContentStore, (content) =>
  content.put({ source: UploadSource.stream(body) }),
).pipe(Effect.provide(contentLive));
const mirrorWrite: Effect.Effect<
  { operation: MirrorOperation },
  StorageError | SourceError,
  StorageMirror | Source
> = Effect.flatMap(StorageMirror, (mirror) => mirror.write({ key: "x", body }));
// @ts-expect-error content sources retain their required environment
const contentMissingSource: Effect.Effect<
  { id: string; metadata: ObjectMetadata },
  StorageError | SourceError
> = contentProgram;
void [
  readOnlyProgram,
  migrationProgram,
  mirroredProgram,
  contentProgram,
  mirrorWrite,
  contentMissingSource,
  StorageCompression.layer,
  StorageEncryption.layer,
  StorageEncryptionCodec,
];
