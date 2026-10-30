import { Context, Effect, Layer } from "effect";

import { Storage as GcsClient } from "@google-cloud/storage";

import { StoragePublicUrls } from "./capabilities.js";
import { storageError } from "./errors.js";
import { resolveConfig } from "./gcs/config.js";
import type { GcsConfig, GcsConnection } from "./gcs/config.js";
import { nativeOperations, preconditions } from "./gcs/native.js";
import type { GcsPreconditions } from "./gcs/native.js";
import { primitives, putObject } from "./gcs/primitives.js";
import { offsetUploads } from "./gcs/resumable.js";
import { validateKey } from "./gcs/validation.js";
import { makePublicUrls } from "./internal/public-urls.js";
import type { PutRequest } from "./requests.js";
import { Storage } from "./storage.js";
import { TransferDriver } from "./transfers/dependencies.js";
import type { TransferEndpoint } from "./transfers/models.js";

export type { GcsConfig } from "./gcs/config.js";
export type { GcsPreconditions } from "./gcs/native.js";
function assemble(connection: GcsConnection) {
  return {
    ...primitives(connection),
    ...connection,
    ...nativeOperations(connection),
    offsets: offsetUploads(connection),
    putObject: <E = never, R = never>(
      request: PutRequest<E, R>,
      conditions: GcsPreconditions = {},
    ) =>
      preconditions(conditions).pipe(
        Effect.flatMap((value) => putObject(connection, request, value)),
      ),
  };
}
export type GcsStorageService = ReturnType<typeof assemble>;
function services(storage: GcsStorageService) {
  const driver: TransferEndpoint = {
    storage,
    maximumObjectBytes: 5 * 1024 ** 4,
    maximumSingleUploadBytes: 256 * 1024 ** 2,
    transportOverheadBytes: 1024 ** 2,
    offsets: storage.offsets,
    validateUpload: ({ key }) => validateKey(key, "upload", true).pipe(Effect.asVoid),
    reconcile: ({ key, operationId, size }) =>
      storage.head({ key }).pipe(
        Effect.map((observed) =>
          observed.size === size &&
          observed.customMetadata?.["effect-storage-operation"] === operationId
            ? observed
            : undefined,
        ),
        Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(undefined)),
      ),
  };
  return Context.make(Storage, storage).pipe(
    Context.add(GcsStorage, storage),
    Context.add(TransferDriver, driver),
  );
}
export class GcsStorage extends Context.Service<GcsStorage, GcsStorageService>()(
  "@effect-kit/storage/gcs/GcsStorage",
) {
  static readonly make = Effect.fnUntraced(function* (input: GcsConfig) {
    const config = yield* resolveConfig(input);
    const client = yield* Effect.try({
      try: () =>
        new GcsClient({
          ...(input.projectId ? { projectId: input.projectId } : {}),
          ...(input.credentials ? { credentials: input.credentials } : {}),
          ...(input.authClient ? { authClient: input.authClient } : {}),
          ...(input.keyFilename ? { keyFilename: input.keyFilename } : {}),
          ...(input.apiEndpoint
            ? {
                apiEndpoint: input.apiEndpoint,
                useAuthWithCustomEndpoint: input.useAuthWithCustomEndpoint ?? true,
              }
            : {}),
          retryOptions: { autoRetry: false, maxRetries: 0 },
        }),
      catch: () => storageError("make", "gcs", { _tag: "InvalidConfiguration", field: "client" }),
    });
    if (client.apiEndpoint.replace(/\/$/u, "") !== config.apiEndpoint)
      return yield* storageError("make", "gcs", {
        _tag: "InvalidConfiguration",
        field: "client.endpoint",
      });
    return assemble({ config, client });
  });
  static readonly makeClient = Effect.fnUntraced(function* (client: GcsClient, input: GcsConfig) {
    const config = yield* resolveConfig(input);
    if (
      !input.storeId ||
      input.credentials ||
      input.authClient ||
      input.keyFilename ||
      client.apiEndpoint.replace(/\/$/u, "") !== config.apiEndpoint ||
      client.retryOptions.autoRetry !== false ||
      client.retryOptions.maxRetries !== 0
    )
      return yield* storageError("makeClient", "gcs", {
        _tag: "InvalidConfiguration",
        field: "client.policy",
      });
    return assemble({ config, client });
  });
  static layer(config: GcsConfig) {
    return Layer.effectContext(Effect.map(GcsStorage.make(config), services));
  }
  static layerClient(client: GcsClient, config: GcsConfig) {
    return Layer.effectContext(Effect.map(GcsStorage.makeClient(client, config), services));
  }
  static publicUrls(baseUrl?: string) {
    return Layer.effect(
      StoragePublicUrls,
      Effect.flatMap(GcsStorage, (storage) =>
        makePublicUrls(
          storage,
          baseUrl ?? `${storage.config.apiEndpoint}/${encodeURIComponent(storage.config.bucket)}/`,
        ),
      ),
    );
  }
}
