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

import { GcsVersions, GcsMetadata, GcsRecovery } from "@effect-kit/storage/gcs/lifecycle";
assert(GcsVersions.make);
assert(GcsMetadata.make);
assert(GcsRecovery.make);

import { GcsProtection, GcsStorageTier } from "@effect-kit/storage/gcs/governance";
assert(GcsProtection.make);
assert(GcsStorageTier.make);

import { GcsAdministration } from "@effect-kit/storage/gcs/admin";
assert(GcsAdministration.make);

import { GcsEvents } from "@effect-kit/storage/gcs/events";
assert(GcsEvents.decode);

assert(Object.keys(await import("@effect-kit/storage/gcs/uploads")).length > 0);
