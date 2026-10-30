import assert from "node:assert/strict";
import { createRequire } from "node:module";

import { Effect, Redacted } from "effect";
import { FetchHttpClient } from "effect/http";

import { SupabaseStorage } from "@effect-kit/storage/supabase";
import { SupabaseSigning } from "@effect-kit/storage/supabase/signing";
const require = createRequire(import.meta.url);
for (const peer of [
  "@vercel/blob",
  "@supabase/storage-js",
  "tus-js-client",
  "@aws-sdk/client-s3",
  "@google-cloud/storage",
  "@azure/storage-blob",
])
  assert.throws(() => require.resolve(peer));
const storage = await Effect.runPromise(
  SupabaseStorage.make({
    url: "https://example.supabase.co",
    bucket: "files",
    storeId: "files",
    auth: { mode: "user", token: Redacted.make("user-jwt") },
  }).pipe(Effect.provide(FetchHttpClient.layer)),
);
assert.equal(storage.provider, "supabase");
assert.equal(storage.offsets, undefined);
assert(SupabaseSigning.layer);
