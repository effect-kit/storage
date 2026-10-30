import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { WasabiNative, WasabiCompliance } from "@effect-kit/storage/wasabi/native";
const require = createRequire(import.meta.url);
for (const peer of [
  "@azure/storage-blob",
  "@google-cloud/storage",
  "@aws-sdk/s3-request-presigner",
  "@aws-sdk/s3-presigned-post",
])
  assert.throws(() => require.resolve(peer));
assert(WasabiNative.make);
assert(WasabiNative.layer);
assert(WasabiCompliance.make);
assert(WasabiCompliance.layer);
