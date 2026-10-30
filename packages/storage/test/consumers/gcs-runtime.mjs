import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { Effect } from "effect";

import { GcsStorage } from "@effect-kit/storage/gcs";
import { GcsPost } from "@effect-kit/storage/gcs/post";
import { GcsSigning } from "@effect-kit/storage/gcs/signing";
const require = createRequire(import.meta.url);
for (const peer of [
  "@azure/storage-blob",
  "@azure/identity",
  "@aws-sdk/client-s3",
  "@effect/platform-node",
])
  assert.throws(() => require.resolve(peer));
const storage = await Effect.runPromise(GcsStorage.make({ bucket: "test-bucket" }));
assert.equal(storage.provider, "gcs");
assert(storage.offsets);
assert(GcsSigning.layer);
assert(GcsPost.layer);
