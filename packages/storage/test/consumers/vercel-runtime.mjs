import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { Effect, Redacted } from "effect";

import { VercelStorage } from "@effect-kit/storage/vercel";
import { VercelClient } from "@effect-kit/storage/vercel/client";
import { VercelSigning } from "@effect-kit/storage/vercel/signing";
import { VercelUploadHandler } from "@effect-kit/storage/vercel/uploads";
const require = createRequire(import.meta.url);
for (const peer of [
  "@supabase/storage-js",
  "tus-js-client",
  "@aws-sdk/client-s3",
  "@google-cloud/storage",
  "@azure/storage-blob",
])
  assert.throws(() => require.resolve(peer));
process.env.VERCEL_BLOB_RETRIES = "0";
const storage = await Effect.runPromise(
  VercelStorage.make({
    storeId: "example",
    access: "private",
    credential: { mode: "token", token: Redacted.make("vercel_blob_rw_example_application") },
  }),
);
assert.equal(storage.provider, "vercel");
assert(VercelSigning.layer);
assert(VercelClient.upload);
assert(VercelUploadHandler.make);

assert(Object.keys(await import("@effect-kit/storage/vercel/authorized-uploads")).length > 0);
