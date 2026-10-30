import { Config, Context, Effect, Layer } from "effect";
import type { Scope } from "effect";

import { S3Client } from "@aws-sdk/client-s3";

import { StorageCopy, StoragePublicUrls } from "./capabilities.js";
import type { StorageError } from "./errors.js";
import { storageError } from "./errors.js";
import type { ObjectMetadata } from "./object.js";
import type { PutRequest } from "./requests.js";
import type { deleteObjects } from "./s3/bulk-delete.js";
import { resolveConfig } from "./s3/config.js";
import type { S3Config, ResolvedS3Config } from "./s3/config.js";
import type { copyObject, copyPart } from "./s3/copy.js";
import type { multipart } from "./s3/multipart.js";
import { validateRef } from "./s3/object-mapping.js";
import type { S3PutOptions } from "./s3/primitives.js";
import type { S3Profile } from "./s3/profile.js";
import { assemble } from "./s3/service.js";
import { makeS3TransferDriver } from "./s3/transfer-driver.js";
import { Storage } from "./storage.js";
import type { StorageService } from "./storage.js";
import { TransferDriver } from "./transfers/dependencies.js";

export type { S3Config, S3Encryption } from "./s3/config.js";
export type { S3CopyRequest } from "./s3/native-models.js";
export type { S3CopyPartRequest } from "./s3/copy.js";
export type { S3DeleteOutcome } from "./s3/bulk-delete.js";
export type { S3MultipartHandle, S3MultipartUploads, S3ChecksumOptions } from "./s3/multipart.js";
export type { S3PutOptions } from "./s3/primitives.js";
export { nativeLimits as s3Limits } from "./s3/native-models.js";

export interface S3StorageService extends StorageService {
  /** Native escape hatch; ownership follows the constructor used to acquire this service. */
  readonly profile?: S3Profile;
  readonly client: S3Client;
  readonly config: ResolvedS3Config;
  readonly multipart: ReturnType<typeof multipart>;
  readonly copyObject: (
    input: Parameters<typeof copyObject>[1],
    source?: S3StorageService,
  ) => ReturnType<typeof copyObject>;
  readonly copyPart: (
    input: Parameters<typeof copyPart>[1],
    source?: S3StorageService,
  ) => ReturnType<typeof copyPart>;
  readonly deleteObjects: (
    input: Parameters<typeof deleteObjects>[1],
  ) => ReturnType<typeof deleteObjects>;
  readonly putObject: <E = never, R = never>(
    input: PutRequest<E, R>,
    options?: S3PutOptions,
  ) => Effect.Effect<ObjectMetadata, StorageError | E, R>;
}

const services = (service: S3StorageService) =>
  Context.make(Storage, service).pipe(
    Context.add(S3Storage, service),
    Context.add(StorageCopy, { copy: service.copyObject }),
    Context.add(TransferDriver, makeS3TransferDriver(service)),
  );

export class S3Storage extends Context.Service<S3Storage, S3StorageService>()(
  "@effect-kit/storage/s3/S3Storage",
) {
  static readonly make = Effect.fnUntraced(function* (
    input: S3Config,
  ): Effect.fn.Return<S3StorageService, StorageError, Scope.Scope> {
    const config = yield* resolveConfig(input);
    const client = yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          new S3Client({
            region: config.region,
            ...(config.endpoint === undefined ? {} : { endpoint: config.endpoint }),
            ...(config.forcePathStyle === undefined
              ? {}
              : { forcePathStyle: config.forcePathStyle }),
            ...(config.credentials === undefined ? {} : { credentials: config.credentials }),
            maxAttempts: 1,
            followRegionRedirects: false,
            requestChecksumCalculation: "WHEN_SUPPORTED",
            responseChecksumValidation: "WHEN_SUPPORTED",
          }),
        catch: () => storageError("make", "s3", { _tag: "InvalidConfiguration", field: "client" }),
      }),
      (owned) =>
        Effect.sync(() => {
          owned.destroy();
        }),
    );
    const { credentials: _credentials, ...publicConfig } = config;
    return assemble({ config: publicConfig, client });
  });

  static readonly makeClient = Effect.fnUntraced(function* (client: S3Client, input: S3Config) {
    const config = yield* resolveConfig(input);
    if (!input.storeId || input.credentials !== undefined)
      return yield* storageError("makeClient", "s3", {
        _tag: "InvalidConfiguration",
        field: !input.storeId ? "storeId" : "credentials",
      });
    const settings = yield* Effect.tryPromise({
      try: async () => ({
        region: await client.config.region(),
        attempts: await client.config.maxAttempts(),
        requestChecksum: await client.config.requestChecksumCalculation(),
        responseChecksum: await client.config.responseChecksumValidation(),
        endpoint: await client.config.endpoint?.(),
      }),
      catch: () =>
        storageError("makeClient", "s3", { _tag: "InvalidConfiguration", field: "client" }),
    });
    if (
      settings.region !== input.region ||
      settings.attempts !== 1 ||
      settings.requestChecksum !== "WHEN_SUPPORTED" ||
      settings.responseChecksum !== "WHEN_SUPPORTED" ||
      client.config.followRegionRedirects ||
      (input.forcePathStyle !== undefined && input.forcePathStyle !== client.config.forcePathStyle)
    )
      return yield* storageError("makeClient", "s3", {
        _tag: "InvalidConfiguration",
        field: "client.policy",
      });
    const endpoint = settings.endpoint;
    const nativeEndpoint = endpoint
      ? new URL(
          `${endpoint.protocol}//${endpoint.hostname}${endpoint.port === undefined ? "" : `:${String(endpoint.port)}`}${endpoint.path || "/"}`,
        ).href
      : undefined;
    if (nativeEndpoint !== (config.endpoint ? new URL(config.endpoint).href : undefined))
      return yield* storageError("makeClient", "s3", {
        _tag: "InvalidConfiguration",
        field: "client.endpoint",
      });
    return assemble({
      config: { ...config, forcePathStyle: client.config.forcePathStyle },
      client,
    });
  });

  static layer(config: S3Config) {
    return Layer.effectContext(Effect.map(S3Storage.make(config), services));
  }

  static layerClient(client: S3Client, config: S3Config) {
    return Layer.effectContext(Effect.map(S3Storage.makeClient(client, config), services));
  }

  static layerConfig(
    config: Config.Config<S3Config> = Config.all({
      bucket: Config.String("S3_BUCKET"),
      region: Config.String("S3_REGION"),
    }),
  ) {
    return Layer.unwrap(
      config.pipe(
        Effect.mapError(() =>
          storageError("layerConfig", "s3", { _tag: "InvalidConfiguration", field: "config" }),
        ),
        Effect.map(S3Storage.layer),
      ),
    );
  }

  static publicUrls(baseUrl: string) {
    return Layer.effect(
      StoragePublicUrls,
      Effect.gen(function* () {
        const storage = yield* S3Storage;
        const base = URL.parse(baseUrl);
        if (
          !base ||
          !["https:", "http:"].includes(base.protocol) ||
          base.username ||
          base.password ||
          base.search ||
          base.hash ||
          !base.pathname.endsWith("/")
        )
          return yield* storageError("publicUrls", "s3", {
            _tag: "InvalidConfiguration",
            field: "baseUrl",
          });
        return StoragePublicUrls.of({
          url: Effect.fnUntraced(function* (ref) {
            yield* validateRef(ref, storage.config, "url");
            if (
              ref.version ||
              ref.key.split("/").some((segment) => segment === "." || segment === "..")
            )
              return yield* storageError("url", "s3", {
                _tag: "Unsupported",
                capability: "version-or-dot-segment-public-url",
              });
            return `${base.href}${ref.key.split("/").map(encodeURIComponent).join("/")}`;
          }),
        });
      }),
    );
  }
}
