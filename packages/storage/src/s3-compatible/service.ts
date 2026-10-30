import type { Scope } from "effect";
import { Context, Effect, Layer } from "effect";

import { S3Client } from "@aws-sdk/client-s3";

import { StorageCopy } from "../capabilities.js";
import type { StorageError } from "../errors.js";
import { storageError } from "../errors.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { assemble } from "../s3/service.js";
import { makeS3TransferDriver } from "../s3/transfer-driver.js";
import { Storage } from "../storage.js";
import { TransferDriver } from "../transfers/dependencies.js";
import { resolveCompatible } from "./config.js";
import type { CompatibleProvider } from "./config.js";

export const makeCompatible = Effect.fnUntraced(function* (
  provider: CompatibleProvider,
  input: Parameters<typeof resolveCompatible>[1],
) {
  const { profile, config } = yield* resolveCompatible(provider, input);
  const mode = profile.checksums ? "WHEN_SUPPORTED" : "WHEN_REQUIRED";
  const client = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        new S3Client({
          region: config.region,
          ...(config.endpoint ? { endpoint: config.endpoint } : {}),
          forcePathStyle: config.forcePathStyle ?? false,
          ...(config.credentials ? { credentials: config.credentials } : {}),
          maxAttempts: 1,
          followRegionRedirects: false,
          requestChecksumCalculation: mode,
          responseChecksumValidation: mode,
        }),
      catch: () =>
        storageError("make", provider, { _tag: "InvalidConfiguration", field: "client" }),
    }),
    (owned) => Effect.sync(() => owned.destroy()),
  );
  const { credentials: _credentials, ...publicConfig } = config;
  return assemble({
    client,
    config: publicConfig,
    profile,
    ...(profile.checksums ? {} : { readChecksums: false, multipartChecksums: false }),
  });
});

export function compatibleLayer<I>(
  tag: Context.Service<I, S3StorageService>,
  make: Effect.Effect<S3StorageService, StorageError, Scope.Scope>,
) {
  return Layer.effectContext(
    Effect.map(make, (storage) =>
      Context.make(tag, storage).pipe(
        Context.add(Storage, storage),
        Context.add(S3Storage, storage),
        Context.add(StorageCopy, { copy: storage.copyObject }),
        Context.add(TransferDriver, makeS3TransferDriver(storage)),
      ),
    ),
  );
}
