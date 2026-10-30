import { Context, Effect, Layer } from "effect";

import { Storage } from "../storage.js";
import type { StorageService } from "../storage.js";
import { TransferDriver } from "../transfers/dependencies.js";

/** Facades expose only the capabilities they enforce. Native protocol/admin services stay private. */
export const storageLayer = <E, R>(make: Effect.Effect<StorageService, E, R>) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const previous = yield* TransferDriver;
      const storage = yield* make;
      const context = Context.make(Storage, storage);

      return Context.add(context, TransferDriver, {
        storage,
        underlyingStoreId: previous.underlyingStoreId ?? previous.storage.storeId,
        ...(previous.underlyingStoreIds ? { underlyingStoreIds: previous.underlyingStoreIds } : {}),
        maximumObjectBytes: previous.maximumObjectBytes,
        maximumSingleUploadBytes: previous.maximumSingleUploadBytes,
        transportOverheadBytes: previous.transportOverheadBytes,
      });
    }),
  );
