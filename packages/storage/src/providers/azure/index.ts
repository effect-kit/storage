import { Context, Effect, Layer, Redacted } from "effect";

import { ContainerClient } from "@azure/storage-blob";

import { StoragePublicUrls } from "../../core/capabilities.js";
import { storageError } from "../../core/errors.js";
import { Storage } from "../../core/storage.js";
import { makePublicUrls } from "../../internal/public-urls.js";
import { TransferDriver } from "../../transfers/dependencies.js";
import type { TransferEndpoint } from "../../transfers/models.js";
import { blockUploads } from "./blocks.js";
import { resolveConfig } from "./config.js";
import type { AzureConfig, AzureConnection } from "./config.js";
import { writeMetadata } from "./mapping.js";
import { nativeOperations } from "./native.js";
import { primitives } from "./primitives.js";
import { validateKey } from "./validation.js";

export type { AzureConfig } from "./config.js";
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
