import { generateKeyPairSync } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { DateTime, Effect, Layer, Redacted } from "effect";

import { StorageSigning } from "../../src/capabilities.js";
import { GcsStorage } from "../../src/gcs.js";
import { GcsPost } from "../../src/gcs/post.js";
import { GcsSigning } from "../../src/gcs/signing.js";

it.live("GCS SDK signs lossless generation URLs and POST policies with explicit authority", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const privateKey = generateKeyPairSync("rsa", { modulusLength: 2048 })
        .privateKey.export({ type: "pkcs8", format: "pem" })
        .toString();
      const provider = GcsStorage.layer({
        bucket: "test-bucket",
        projectId: "test-project",
        storeId: "signed",
        credentials: {
          client_email: "storage@test-project.iam.gserviceaccount.com",
          private_key: privateKey,
        },
      });
      const expires = DateTime.makeUnsafe(Math.floor((Date.now() + 30_000) / 1000) * 1000);
      const context = yield* Layer.build(
        Layer.merge(GcsSigning.layer({ authorityExpiresAt: expires }), GcsPost.layer()).pipe(
          Layer.provide(provider),
        ),
      );
      const grant = yield* Effect.flatMap(StorageSigning, (service) =>
        service.sign({
          ref: {
            storeId: "signed",
            key: "a /?",
            version: { _tag: "GcsGeneration", id: "9007199254740993" },
          },
          method: "GET",
          expiresInSeconds: 60,
        }),
      ).pipe(Effect.provide(context));
      assert.strictEqual(
        new URL(Redacted.value(grant.url)).searchParams.get("generation"),
        "9007199254740993",
      );
      assert.strictEqual(DateTime.toEpochMillis(grant.expiresAt), DateTime.toEpochMillis(expires));
      assert.isFalse(JSON.stringify(grant).includes("X-Goog-Signature"));
      const post = yield* Effect.flatMap(GcsPost, (service) =>
        service.create({ key: "upload", maxBytes: 100, minBytes: 1, expiresInSeconds: 60 }),
      ).pipe(Effect.provide(context));
      const policy = JSON.parse(
        Buffer.from(Redacted.value(post.fields).policy ?? "", "base64").toString("utf8"),
      ) as { conditions: unknown[] };
      assert.deepInclude(policy.conditions, ["content-length-range", 1, 100]);
    }),
  ),
);
