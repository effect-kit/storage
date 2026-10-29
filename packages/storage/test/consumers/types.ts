import { Context, Effect, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { ObjectKey, ObjectSize, StorageSigning } from "@effect-kit/storage";
import type { ObjectMetadata, StorageError, StorageService, Storage } from "@effect-kit/storage";
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
