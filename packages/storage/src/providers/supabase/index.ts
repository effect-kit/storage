import { Context, Effect, Layer } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";

import { StoragePublicUrls } from "../../core/capabilities.js";
import { Storage } from "../../core/storage.js";
import { makePublicUrls } from "../../internal/public-urls.js";
import { TransferDriver } from "../../transfers/dependencies.js";
import type { TransferEndpoint } from "../../transfers/models.js";
import { resolveConfig } from "./config.js";
import type { SupabaseConfig } from "./config.js";
import { validateKey, nativeHeaders } from "./mapping.js";
import { primitives } from "./primitives.js";
import { negotiateTus, tusUploads } from "./tus.js";

export type { SupabaseConfig } from "./config.js";
const make = Effect.fnUntraced(function* (input: SupabaseConfig) {
  const connection = { config: yield* resolveConfig(input), http: yield* HttpClient.HttpClient };
  const extensions = input.resumable ? yield* negotiateTus(connection) : [];
  return {
    ...primitives(connection),
    ...connection,
    ...(input.resumable ? { offsets: tusUploads(connection, extensions) } : {}),
  };
});
export type SupabaseStorageService = Effect.Success<ReturnType<typeof make>>;
export class SupabaseStorage extends Context.Service<SupabaseStorage, SupabaseStorageService>()(
  "@effect-kit/storage/supabase/SupabaseStorage",
) {
  static readonly make = make;
  static layerHttp(config: SupabaseConfig) {
    return Layer.effectContext(
      Effect.map(make(config), (storage) => {
        const endpoint: TransferEndpoint = {
          storage,
          offsets: storage.offsets,
          maximumObjectBytes: storage.config.maxObjectBytes,
          maximumSingleUploadBytes: storage.config.maxObjectBytes,
          transportOverheadBytes: 1024 ** 2,
          validateUpload: ({ key, headers }) =>
            validateKey(key, "upload").pipe(
              Effect.andThen(nativeHeaders(headers, "upload")),
              Effect.asVoid,
            ),
          reconcile: ({ key, operationId, size }) =>
            storage.head({ key }).pipe(
              Effect.map((found) =>
                found.size === size &&
                found.customMetadata?.["effect-storage-operation"] === operationId
                  ? found
                  : undefined,
              ),
              Effect.catchReason("StorageError", "NotFound", () => Effect.undefined),
            ),
        };
        return Context.make(Storage, storage).pipe(
          Context.add(SupabaseStorage, storage),
          Context.add(TransferDriver, endpoint),
        );
      }),
    );
  }
  static layer(config: SupabaseConfig) {
    return SupabaseStorage.layerHttp(config).pipe(Layer.provide(FetchHttpClient.layer));
  }
  static publicUrls() {
    return Layer.effect(
      StoragePublicUrls,
      Effect.flatMap(SupabaseStorage, (storage) =>
        makePublicUrls(
          storage,
          `${storage.config.baseUrl}/object/public/${encodeURIComponent(storage.config.bucket)}/`,
        ),
      ),
    );
  }
}
