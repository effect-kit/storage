import { Clock, DateTime, Effect, Layer, Redacted } from "effect";

import {
  BlobSASPermissions,
  SASProtocol,
  type StorageSharedKeyCredential,
  generateBlobSASQueryParameters,
} from "@azure/storage-blob";
import type { UserDelegationKey } from "@azure/storage-blob";

import { AzureStorage } from "../azure.js";
import { StorageSigning } from "../capabilities.js";
import type { SignedRequest } from "../capabilities.js";
import { storageError } from "../errors.js";
import { signingRequest } from "../internal/signing-request.js";
import { target } from "./mapping.js";

export type AzureSigningAuthority =
  | {
      readonly kind: "shared-key";
      readonly credential: StorageSharedKeyCredential;
      readonly expiresAt?: DateTime.Utc;
    }
  | {
      readonly kind: "user-delegation";
      readonly accountName: string;
      readonly key: Redacted.Redacted<UserDelegationKey>;
    };

export const AzureSigning = {
  layer(authority: AzureSigningAuthority) {
    return Layer.effect(
      StorageSigning,
      Effect.gen(function* () {
        const storage = yield* AzureStorage;
        return StorageSigning.of({
          sign: Effect.fnUntraced(function* (input) {
            yield* signingRequest(input, "azure");
            const address = yield* target(storage, { ref: input.ref }, "sign");
            if (
              storage.config.encryption ||
              storage.config.leaseId ||
              (input.ref.version && input.method !== "GET" && input.method !== "HEAD")
            )
              return yield* storageError(
                "sign",
                "azure",
                {
                  _tag: "Unsupported",
                  capability: "signing-encryption-lease-or-versioned-mutation",
                },
                "not-started",
              );
            const account =
              authority.kind === "shared-key"
                ? authority.credential.accountName
                : authority.accountName;
            if (storage.client.accountName !== account)
              return yield* storageError(
                "sign",
                "azure",
                { _tag: "InvalidConfiguration", field: "signing-account" },
                "not-started",
              );
            const now = yield* Clock.currentTimeMillis;
            const key =
              authority.kind === "user-delegation" ? Redacted.value(authority.key) : undefined;
            const authorityExpiry = key
              ? key.signedExpiresOn.getTime()
              : authority.kind === "shared-key" && authority.expiresAt
                ? DateTime.toEpochMillis(authority.expiresAt)
                : Infinity;
            const expiry =
              Math.floor(Math.min(now + input.expiresInSeconds * 1000, authorityExpiry) / 1000) *
              1000;
            if (
              !Number.isSafeInteger(input.expiresInSeconds) ||
              input.expiresInSeconds <= 0 ||
              input.expiresInSeconds > 604800 ||
              expiry <= now ||
              (key && key.signedStartsOn.getTime() > now) ||
              new URL(storage.config.containerUrl).protocol !== "https:"
            )
              return yield* storageError(
                "sign",
                "azure",
                { _tag: "InvalidInput", field: "expiry-or-endpoint" },
                "not-started",
              );
            const query = yield* Effect.try({
              try: () => {
                const settings = {
                  containerName: storage.client.containerName,
                  blobName: input.ref.key,
                  permissions: BlobSASPermissions.parse(
                    input.method === "GET" || input.method === "HEAD"
                      ? "r"
                      : input.method === "DELETE"
                        ? "d"
                        : "cw",
                  ),
                  protocol: SASProtocol.Https,
                  startsOn: new Date(Math.max(now - 60_000, key?.signedStartsOn.getTime() ?? 0)),
                  expiresOn: new Date(expiry),
                  ...(input.ref.version
                    ? input.ref.version._tag === "AzureSnapshot"
                      ? { snapshotTime: input.ref.version.id }
                      : { versionId: input.ref.version.id }
                    : {}),
                };
                return authority.kind === "shared-key"
                  ? generateBlobSASQueryParameters(settings, authority.credential)
                  : generateBlobSASQueryParameters(
                      settings,
                      Redacted.value(authority.key),
                      authority.accountName,
                    );
              },
              catch: () =>
                storageError(
                  "sign",
                  "azure",
                  { _tag: "InvalidConfiguration", field: "signing-authority" },
                  "not-started",
                ),
            });
            const url = new URL(address.client.url);
            url.search = query.toString();
            if (input.ref.version)
              url.searchParams.set(
                input.ref.version._tag === "AzureSnapshot" ? "snapshot" : "versionid",
                input.ref.version.id,
              );
            return {
              method: input.method,
              ref: input.ref,
              url: Redacted.make(url.href),
              headers: Redacted.make(
                input.method === "PUT" ? { "x-ms-blob-type": "BlockBlob" } : {},
              ),
              expiresAt: DateTime.makeUnsafe(expiry),
              constraints: [
                {
                  kind: "permissions",
                  enforcement: "provider",
                  value:
                    input.method === "GET" || input.method === "HEAD"
                      ? "r"
                      : input.method === "DELETE"
                        ? "d"
                        : "cw",
                },
              ],
            } satisfies SignedRequest;
          }),
        });
      }),
    );
  },
};
