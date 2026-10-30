// Run after pnpm build. This example uses ephemeral memory and owned temporary files.
import { Effect, Layer } from "effect";

import { StorageCache } from "../dist/cache.js";
import { StoragePrefix, StorageReadOnly } from "../dist/composition.js";
import { StorageCompression, StorageRepresentation } from "../dist/compression.js";
import { Storage } from "../dist/index.js";
import { layer as platform } from "../dist/node.js";
import { MemoryStorage } from "../dist/testing.js";

const ProviderLive = MemoryStorage.layer({ storeId: "composition-example" });
const TenantLive = StoragePrefix.layer({ prefix: "tenant-a/" }).pipe(Layer.provide(ProviderLive));
const CachedLive = StorageCache.layer({
  capacity: 100,
  timeToLive: "30 seconds",
  objects: { capacity: 4, maxObjectBytes: 1024 * 1024 },
}).pipe(Layer.provide(TenantLive));
const StorageLive = StorageCompression.layer({
  format: "gzip",
  maxDecodedBytes: 1024 * 1024,
  maxStoredBytes: 2 * 1024 * 1024,
}).pipe(Layer.provide(CachedLive));

const result = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* Storage;
      yield* storage.put({
        key: "hello.txt",
        body: new TextEncoder().encode("hello from composed storage"),
        contentType: "text/plain",
      });
      const text = yield* Storage.getText({ key: "hello.txt", maxBytes: 1024 });
      const representation = yield* StorageRepresentation;
      const sizes = yield* representation.inspect({ key: "hello.txt" });
      const readOnly = yield* StorageReadOnly.make;
      const rejected = yield* Effect.result(readOnly.delete({ key: "hello.txt" }));
      return {
        text,
        logicalBytes: sizes.logical.size,
        storedBytes: sizes.stored.size,
        readOnlyRejected: rejected._tag === "Failure",
      };
    }),
  ).pipe(Effect.provide(StorageLive), Effect.provide(platform)),
);
process.stdout.write(`${JSON.stringify(result)}\n`);
