import { Effect, Layer } from "effect";

import { StorageSharedKeyCredential } from "@azure/storage-blob";
import { AzureStorage } from "@effect-kit/storage/azure";
import { AzureSigning } from "@effect-kit/storage/azure/signing";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";

const provider = AzureStorage.layer({ containerUrl: "https://example.blob.core.windows.net/test" });
export const signing = AzureSigning.layer({
  kind: "shared-key",
  credential: new StorageSharedKeyCredential("example", "dGVzdA=="),
}).pipe(Layer.provide(provider));
export const upload = Effect.gen(function* () {
  const transfers = yield* Transfers;
  return yield* transfers.upload({
    key: "example",
    source: UploadSource.bytes(new Uint8Array([1])),
    strategy: "blocks",
  });
});

import { AzureVersions, AzureMetadata, AzureTags } from "@effect-kit/storage/azure/lifecycle";
export const lifecycleCapabilities = [AzureVersions, AzureMetadata, AzureTags] as const;

import { AzureProtection } from "@effect-kit/storage/azure/governance";
export const governanceCapabilities = [AzureProtection] as const;

import { AzureAdministration } from "@effect-kit/storage/azure/admin";
export const adminCapabilities = [AzureAdministration] as const;

import { AzureArchive } from "@effect-kit/storage/azure/archive";
export const archiveCapabilities = [AzureArchive] as const;

import { AzureAccountAdministration } from "@effect-kit/storage/azure/account-admin";
export const account_adminCapabilities = [AzureAccountAdministration] as const;

import { AzureEvents } from "@effect-kit/storage/azure/events";
export const eventsCapabilities = [AzureEvents] as const;
