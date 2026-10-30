import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { TigrisAdministration, TigrisRecovery } from "@effect-kit/storage/tigris/native";
const require = createRequire(import.meta.url);
for (const peer of [
  "@azure/storage-blob",
  "@google-cloud/storage",
  "@aws-sdk/s3-request-presigner",
  "@aws-sdk/s3-presigned-post",
])
  assert.throws(() => require.resolve(peer));
assert(TigrisAdministration.make);
assert(TigrisAdministration.layer);
assert(TigrisRecovery.make);
assert(TigrisRecovery.layer);
