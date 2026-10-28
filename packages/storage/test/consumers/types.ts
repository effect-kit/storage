import { Context, Effect, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { ObjectKey, ObjectSize, StorageSigning } from "@effect-kit/storage";
import type { ObjectMetadata, StorageError, StorageService, Storage } from "@effect-kit/storage";
import { Faults, MemoryStorage } from "@effect-kit/storage/testing";

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
