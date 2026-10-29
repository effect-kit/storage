import { DateTime, Effect } from "effect";

import { S3Client } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import type { S3StorageService } from "../s3.js";
import { request } from "./transport.js";

export const signingCredentials = Effect.fnUntraced(function* (
  storage: S3StorageService,
  expiresInSeconds: number,
) {
  const now = yield* DateTime.now;
  const credentials = yield* request(storage, "sign", false, () =>
    storage.client.config.credentials(),
  );
  const seconds = Math.min(
    expiresInSeconds,
    credentials.expiration
      ? Math.floor((credentials.expiration.getTime() - DateTime.toEpochMillis(now)) / 1000)
      : expiresInSeconds,
  );
  if (seconds < 1) return yield* storageError("sign", "s3", { _tag: "Expired" });

  // Freeze the exact resolved identity for this signature; the next grant resolves afresh.
  // Signing has no body to checksum. Data-plane clients retain WHEN_SUPPORTED protection.
  const client = new S3Client({
    region: storage.config.region,
    ...(storage.config.endpoint === undefined ? {} : { endpoint: storage.config.endpoint }),
    ...(storage.config.forcePathStyle === undefined
      ? {}
      : { forcePathStyle: storage.config.forcePathStyle }),
    credentials,
    requestChecksumCalculation: "WHEN_REQUIRED",
    maxAttempts: 1,
    requestHandler: {
      handle: async () => {
        throw new Error("Signing must not send a network request");
      },
    },
    systemClockOffset: DateTime.toEpochMillis(now) - Date.now(),
  });
  return { client, now, seconds, expiresAt: DateTime.add(now, { seconds }) };
});
