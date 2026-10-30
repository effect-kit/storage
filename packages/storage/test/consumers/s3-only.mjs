import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { createRequire } from "node:module";

import { Context, Effect, Layer } from "effect";

import { S3Storage, s3Limits } from "@effect-kit/storage/s3";
import { TransferDriver } from "@effect-kit/storage/transfers";

const require = createRequire(import.meta.url);
for (const peer of ["@aws-sdk/s3-request-presigner", "@aws-sdk/s3-presigned-post"])
  assert.throws(() => require.resolve(peer));
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const services = yield* Layer.build(
        S3Storage.layer({
          bucket: "artifact-test",
          region: "us-east-1",
          credentials: {
            accessKeyId: randomBytes(12).toString("hex"),
            secretAccessKey: randomBytes(24).toString("hex"),
          },
        }),
      );
      const storage = Context.get(services, S3Storage);
      assert.equal(storage.provider, "s3");
      assert.equal(storage.capabilities.versionedReads, "history");
      assert.equal(s3Limits.maximumParts, 10000);
      assert.equal(Context.get(services, TransferDriver).multipart, storage.multipart);
    }),
  ),
);

const { SupabaseS3Storage } = await import("@effect-kit/storage/supabase/s3");
assert(SupabaseS3Storage.layer);
