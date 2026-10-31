import { Clock, DateTime, Effect, Layer, Redacted } from "effect";

import { StorageSigning } from "../core/capabilities.js";
import type { SignedRequest } from "../core/capabilities.js";
import type { StorageError } from "../core/errors.js";
import { Storage, type StorageService } from "../core/storage.js";
import { UploadGrantDriver } from "./driver.js";
import { failUpload } from "./schema.js";
import type { UploadRecord } from "./schema.js";

export const signedUploadDriver = (
  issue: (
    record: UploadRecord,
    seconds: number,
    storage: StorageService,
  ) => Effect.Effect<SignedRequest, StorageError>,
) =>
  Layer.effect(
    UploadGrantDriver,
    Effect.gen(function* () {
      const storage = yield* Storage;
      return UploadGrantDriver.of({
        begin: (record) =>
          !["auto", "single"].includes(record.input.strategy ?? "auto")
            ? Effect.fail(failUpload("Unsupported"))
            : Effect.succeed(null),
        grant: Effect.fnUntraced(function* (record, expiresAt) {
          const seconds = Math.max(
            1,
            Math.floor((expiresAt - (yield* Clock.currentTimeMillis)) / 1000),
          );
          const grant = yield* issue(record, seconds, storage);
          return {
            _tag: "SignedRequest",
            constraints: grant.constraints,
            method: "PUT",
            url: Redacted.value(grant.url),
            headers: Redacted.value(grant.headers),
            expiresAt: DateTime.toEpochMillis(grant.expiresAt),
          };
        }),
        abort: () => Effect.void,
      });
    }),
  );
export const SignedUploadGrants = Layer.unwrap(
  Effect.map(StorageSigning, (signing) =>
    signedUploadDriver((record, expiresInSeconds, storage) =>
      signing.sign({
        ref: { storeId: storage.storeId, key: record.stagingKey },
        method: "PUT",
        expiresInSeconds,
      }),
    ),
  ),
);
