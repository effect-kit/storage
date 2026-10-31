import { Context, Effect, Layer } from "effect";

import { StoragePublicUrls } from "../../core/capabilities.js";
import { storageError } from "../../core/errors.js";
import { Storage } from "../../core/storage.js";
import { makePublicUrls } from "../../internal/public-urls.js";
import { TransferDriver } from "../../transfers/dependencies.js";
import { resolveConfig } from "./config.js";
import type { VercelConfig } from "./config.js";
import { validateKey, writeOptions } from "./mapping.js";
import { multipart } from "./multipart.js";
import { primitives } from "./primitives.js";

export type { VercelConfig } from "./config.js";
const make = Effect.fnUntraced(function* (input: VercelConfig) {
  const connection = { config: yield* resolveConfig(input) };
  return { ...primitives(connection), ...connection, multipart: multipart(connection) };
});
export type VercelStorageService = Effect.Success<ReturnType<typeof make>>;
export class VercelStorage extends Context.Service<VercelStorage, VercelStorageService>()(
  "@effect-kit/storage/vercel/VercelStorage",
) {
  static readonly make = make;
  static layer(config: VercelConfig) {
    return Layer.effectContext(
      Effect.map(make(config), (storage) =>
        Context.make(Storage, storage).pipe(
          Context.add(VercelStorage, storage),
          Context.add(TransferDriver, {
            storage,
            multipart: storage.multipart,
            maximumObjectBytes: storage.config.maxObjectBytes,
            maximumSingleUploadBytes: Math.min(storage.config.maxObjectBytes, 256 * 1024 ** 2),
            transportOverheadBytes: 1024 ** 2,
            validateUpload: ({ key, headers }) =>
              validateKey(key, "upload").pipe(
                Effect.andThen(writeOptions(headers, "upload")),
                Effect.asVoid,
              ),
            // No public inventory/abort, operation marker or durable handle codec. Never adopt by pathname alone.
          }),
        ),
      ),
    );
  }
  static publicUrls() {
    return Layer.effect(
      StoragePublicUrls,
      Effect.gen(function* () {
        const storage = yield* VercelStorage;
        if (storage.config.access !== "public")
          return yield* storageError("publicUrls", "vercel", {
            _tag: "Unsupported",
            capability: "private-store-public-url",
          });
        return yield* makePublicUrls(
          storage,
          `${storage.config.apiOrigin ?? `https://${storage.config.storeId}.public.blob.vercel-storage.com`}/`,
        );
      }),
    );
  }
}
