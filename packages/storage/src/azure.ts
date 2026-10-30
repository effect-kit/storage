import { Context, Effect, Layer, Redacted } from "effect";

import { ContainerClient } from "@azure/storage-blob";

import { blockUploads } from "./azure/blocks.js";
import { resolveConfig } from "./azure/config.js";
import type { AzureConfig, AzureConnection } from "./azure/config.js";
import { writeMetadata } from "./azure/mapping.js";
import { nativeOperations } from "./azure/native.js";
import { primitives } from "./azure/primitives.js";
import { validateKey } from "./azure/validation.js";
import { StoragePublicUrls } from "./capabilities.js";
import { storageError } from "./errors.js";
import { makePublicUrls } from "./internal/public-urls.js";
import { Storage } from "./storage.js";
import { TransferDriver } from "./transfers/dependencies.js";
import type { TransferEndpoint } from "./transfers/models.js";

export type { AzureConfig } from "./azure/config.js";
function assemble(connection: AzureConnection) {
  return {
    ...primitives(connection),
    ...connection,
    ...nativeOperations(connection),
    blocks: blockUploads(connection),
  };
}
export type AzureStorageService = ReturnType<typeof assemble>;
function services(storage: AzureStorageService) {
  const driver: TransferEndpoint = {
    storage,
    maximumObjectBytes: 4000 * 1024 ** 2 * 50_000,
    maximumSingleUploadBytes: 256 * 1024 ** 2,
    transportOverheadBytes: 1024 ** 2,
    blocks: storage.blocks,
    validateUpload: ({ key, headers }) =>
      validateKey(key, "upload", true).pipe(Effect.andThen(writeMetadata(headers)), Effect.asVoid),
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
    Context.add(AzureStorage, storage),
    Context.add(TransferDriver, driver),
  );
}
export class AzureStorage extends Context.Service<AzureStorage, AzureStorageService>()(
  "@effect-kit/storage/azure/AzureStorage",
) {
  static readonly make = Effect.fnUntraced(function* (input: AzureConfig) {
    const config = yield* resolveConfig(input);
    const client = yield* Effect.try({
      try: () =>
        new ContainerClient(
          Redacted.isRedacted(input.containerUrl)
            ? Redacted.value(input.containerUrl)
            : input.containerUrl,
          input.credential,
          { retryOptions: { maxTries: 1 }, keepAliveOptions: { enable: true } },
        ),
      catch: () => storageError("make", "azure", { _tag: "InvalidConfiguration", field: "client" }),
    });
    return assemble({ config, client });
  });
  static readonly makeClient = Effect.fnUntraced(function* (
    client: ContainerClient,
    input: AzureConfig,
    policy: { readonly sdkRetries: "disabled" },
  ) {
    const config = yield* resolveConfig(input);
    const url = new URL(client.url);
    url.search = "";
    if (
      !input.storeId ||
      input.credential ||
      url.href.replace(/\/$/u, "") !== config.containerUrl ||
      policy?.sdkRetries !== "disabled"
    )
      return yield* storageError("makeClient", "azure", {
        _tag: "InvalidConfiguration",
        field: "client.policy",
      });
    return assemble({ config, client });
  });
  static layer(config: AzureConfig) {
    return Layer.effectContext(Effect.map(AzureStorage.make(config), services));
  }
  static layerClient(
    client: ContainerClient,
    config: AzureConfig,
    policy: { readonly sdkRetries: "disabled" },
  ) {
    return Layer.effectContext(
      Effect.map(AzureStorage.makeClient(client, config, policy), services),
    );
  }
  static publicUrls(baseUrl?: string) {
    return Layer.effect(
      StoragePublicUrls,
      Effect.flatMap(AzureStorage, (storage) =>
        makePublicUrls(storage, baseUrl ?? `${storage.config.containerUrl}/`),
      ),
    );
  }
}
