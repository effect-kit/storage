import { Clock, DateTime, Effect, Layer, Redacted } from "effect";

import { StorageSigning } from "../../core/capabilities.js";
import type { SignedRequest } from "../../core/capabilities.js";
import { storageError } from "../../core/errors.js";
import { signingRequest } from "../../internal/signing-request.js";
import { GcsStorage } from "./index.js";
import { target } from "./mapping.js";

/** Explicitly selected signing authority; ADC data access alone need not permit signBlob. */
export const GcsSigning = {
  layer(options: { readonly authorityExpiresAt?: DateTime.Utc } = {}) {
    return Layer.effect(
      StorageSigning,
      Effect.gen(function* () {
        const storage = yield* GcsStorage;
        return StorageSigning.of({
          sign: Effect.fnUntraced(function* (request) {
            yield* signingRequest(request, "gcs");
            if (storage.config.customerKey)
              return yield* storageError(
                "sign",
                "gcs",
                { _tag: "Unsupported", capability: "customer-key-signing" },
                "not-started",
              );
            const address = yield* target(storage, { ref: request.ref }, "sign");
            if (request.method === "PUT" && (request.ref.version || storage.config.kmsKeyName))
              return yield* storageError(
                "sign",
                "gcs",
                { _tag: "Unsupported", capability: "generation-or-kms-write-signing" },
                "not-started",
              );
            const now = yield* Clock.currentTimeMillis;
            const expiry =
              Math.floor(
                Math.min(
                  now + request.expiresInSeconds * 1000,
                  options.authorityExpiresAt
                    ? DateTime.toEpochMillis(options.authorityExpiresAt)
                    : Infinity,
                ) / 1000,
              ) * 1000;
            if (
              !Number.isSafeInteger(request.expiresInSeconds) ||
              request.expiresInSeconds <= 0 ||
              request.expiresInSeconds > 604800 ||
              expiry <= now ||
              storage.config.apiEndpoint !== "https://storage.googleapis.com"
            )
              return yield* storageError(
                "sign",
                "gcs",
                { _tag: "InvalidInput", field: "expiry-or-endpoint" },
                "not-started",
              );
            if (request.method === "HEAD")
              return yield* storageError(
                "sign",
                "gcs",
                { _tag: "Unsupported", capability: "head-signing" },
                "not-started",
              );
            const actions = { GET: "read", PUT: "write", DELETE: "delete" } as const;
            const action = actions[request.method];
            const [url] = yield* Effect.tryPromise({
              try: () =>
                storage.client
                  .bucket(storage.config.bucket)
                  .file(address.ref.key)
                  .getSignedUrl({
                    version: "v4",
                    action,
                    expires: expiry,
                    ...(address.ref.version
                      ? { queryParams: { generation: address.ref.version.id } }
                      : {}),
                  }),
              catch: () => storageError("sign", "gcs", { _tag: "AccessDenied" }, "not-started"),
            });
            return {
              method: request.method,
              url: Redacted.make(url),
              headers: Redacted.make({}),
              ref: address.ref,
              expiresAt: DateTime.makeUnsafe(expiry),
              constraints: [],
            } satisfies SignedRequest;
          }),
        });
      }),
    );
  },
};
