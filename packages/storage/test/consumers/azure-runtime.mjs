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
