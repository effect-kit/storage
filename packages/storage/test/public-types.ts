import { Context, Effect, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { StorageSigning } from "../src/index.js";
import type { ObjectMetadata, StorageError, StorageService, Storage } from "../src/index.js";
import { Faults, MemoryStorage } from "../src/testing.js";

class SourceDependency extends Context.Service<SourceDependency, { readonly bytes: Uint8Array }>()(
  "test/source",
) {}
class SourceFailure extends Schema.TaggedError<SourceFailure>()("SourceFailure", {}) {}

const source = Stream.unwrap(
  Effect.map(SourceDependency, (dependency) =>
    Stream.concat(Stream.succeed(dependency.bytes), Stream.fail(new SourceFailure())),
  ),
);
declare const adapter: StorageService;
const upload = adapter.put({ key: "source", body: source });
const preserved: Effect.Effect<ObjectMetadata, StorageError | SourceFailure, SourceDependency> =
  upload;
// @ts-expect-error source requirements cannot disappear
const missingDependency: Effect.Effect<ObjectMetadata, StorageError | SourceFailure> = upload;
// @ts-expect-error source failures cannot disappear
const missingFailure: Effect.Effect<ObjectMetadata, StorageError, SourceDependency> = upload;

const memoryUpload = MemoryStorage.make().pipe(
  Effect.flatMap((storage) => storage.put({ key: "source", body: source })),
);
const memoryPreserved: Effect.Effect<
  ObjectMetadata,
  StorageError | SourceFailure,
  SourceDependency
> = memoryUpload;
const decorated = Faults.make(adapter, [{ operation: "put" }]).pipe(
  Effect.flatMap((faults) => faults.storage.put({ key: "source", body: source })),
);
const faultsPreserved: Effect.Effect<
  ObjectMetadata,
  StorageError | SourceFailure,
  SourceDependency
> = decorated;

const download = adapter.get({ key: "scoped" });
// @ts-expect-error acquiring a response requires a scope
const unscoped: Effect.Effect<unknown, StorageError> = download;
declare const body: Stream.Stream<Uint8Array, StorageError, Scope.Scope>;
// @ts-expect-error consuming an acquired body also requires a scope
const unscopedBody: Effect.Effect<unknown, StorageError> = Stream.runDrain(body);
const signing = StorageSigning.use((signer) =>
  signer.sign({
    ref: { storeId: "store", key: "key" },
    method: "GET",
    expiresInSeconds: 60,
  }),
);
// @ts-expect-error Storage alone does not provide signing
const unsigned: Effect.Effect<unknown, StorageError, Storage> = signing;

class Avatars extends Context.Service<Avatars, StorageService>()("test/avatars") {}
class Documents extends Context.Service<Documents, StorageService>()("test/documents") {}
const multipleStores = Effect.gen(function* () {
  const avatars = yield* Avatars;
  const documents = yield* Documents;
  return [avatars.storeId, documents.storeId];
});
const named: Effect.Effect<string[], never, Avatars | Documents> = multipleStores;

void [
  preserved,
  missingDependency,
  missingFailure,
  memoryPreserved,
  faultsPreserved,
  unscoped,
  unscopedBody,
  unsigned,
  named,
];
