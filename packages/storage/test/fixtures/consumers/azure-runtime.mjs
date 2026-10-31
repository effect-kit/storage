import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { Effect } from "effect";

import { AzureStorage } from "@effect-kit/storage/azure";
import { AzureSigning } from "@effect-kit/storage/azure/signing";
const require = createRequire(import.meta.url);
for (const peer of [
  "@google-cloud/storage",
  "@azure/identity",
  "@aws-sdk/client-s3",
  "@effect/platform-node",
])
  assert.throws(() => require.resolve(peer));
const storage = await Effect.runPromise(
  AzureStorage.make({ containerUrl: "https://example.blob.core.windows.net/test" }),
);
assert.equal(storage.provider, "azure");
assert(storage.blocks);
assert(AzureSigning.layer);

import { AzureVersions, AzureMetadata, AzureTags } from "@effect-kit/storage/azure/lifecycle";
assert(AzureVersions.make);
assert(AzureMetadata.make);
assert(AzureTags.make);

import { AzureProtection } from "@effect-kit/storage/azure/governance";
assert(AzureProtection.make);

import { AzureAdministration } from "@effect-kit/storage/azure/admin";
assert(AzureAdministration.make);

import { AzureArchive } from "@effect-kit/storage/azure/archive";
assert(AzureArchive.make);

import { AzureAccountAdministration } from "@effect-kit/storage/azure/account-admin";
assert(AzureAccountAdministration.make);

import { AzureEvents } from "@effect-kit/storage/azure/events";
assert(AzureEvents.decode);

assert(Object.keys(await import("@effect-kit/storage/azure/uploads")).length > 0);
