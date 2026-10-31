import { Context, Effect, Layer } from "effect";

import { Storage as GcsClient } from "@google-cloud/storage";

import { StoragePublicUrls } from "../../core/capabilities.js";
import { storageError } from "../../core/errors.js";
import type { PutRequest } from "../../core/requests.js";
import { Storage } from "../../core/storage.js";
import { makePublicUrls } from "../../internal/public-urls.js";
import { TransferDriver } from "../../transfers/dependencies.js";
import type { TransferEndpoint } from "../../transfers/models.js";
import { resolveConfig } from "./config.js";
import type { GcsConfig, GcsConnection } from "./config.js";
import { nativeOperations, preconditions } from "./native.js";
import type { GcsPreconditions } from "./native.js";
import { primitives, putObject } from "./primitives.js";
import { offsetUploads } from "./resumable.js";
import { validateKey } from "./validation.js";

export type { GcsConfig } from "./config.js";
export type { GcsPreconditions } from "./native.js";
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
