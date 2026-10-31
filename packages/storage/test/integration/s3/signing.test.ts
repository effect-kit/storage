import { randomBytes } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { DateTime, Effect, Layer, Redacted, Result } from "effect";

import { StoragePublicUrls, StorageSigning } from "../../../src/index.js";
import { S3Storage } from "../../../src/providers/s3/index.js";
import { S3Post } from "../../../src/providers/s3/post.js";
import { S3Signing } from "../../../src/providers/s3/signing.js";
import { fixtureCredentials } from "../../fixtures/s3/http-fixture.js";

it.effect(
  "signed PUT returns signed headers, preserves conditions and does not sign an empty-body checksum",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* S3Storage.make({
          bucket: "signing-tests",
          region: "us-east-1",
          credentials: fixtureCredentials(),
        });
        const signing = S3Signing.make(storage);
        const grant = yield* signing.sign({
          ref: { storeId: storage.storeId, key: "literal/%2F/é" },
          method: "PUT",
          expiresInSeconds: 60,
          headers: { contentType: "text/plain", customMetadata: { label: "value" } },
          condition: { _tag: "CreateOnly" },
        });
        const url = new URL(Redacted.value(grant.url));
        const required = Redacted.value(grant.headers);
        assert.strictEqual(required["content-type"], "text/plain");
        assert.strictEqual(required["if-none-match"], "*");
        assert.strictEqual(required["x-amz-meta-label"], "value");
        assert.include(url.searchParams.get("X-Amz-SignedHeaders") ?? "", "content-type");
        assert.isFalse(url.searchParams.has("x-amz-checksum-crc32"));
        assert.include(url.pathname, "%252F/%C3%A9");
        assert.notInclude(JSON.stringify(grant), "X-Amz-Signature");
      }),
    ),
);

it.effect(
  "grants cap expiry to the exact resolved temporary credentials and reevaluate providers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const identity = fixtureCredentials();
        const token = randomBytes(12).toString("hex");
        let refreshes = 0;
        const storage = yield* S3Storage.make({
          bucket: "signing-tests",
          region: "us-east-1",
          credentials: async () => {
            refreshes++;
            return {
              ...identity,
              sessionToken: token,
              expiration: new Date(DateTime.toEpochMillis(now) + 30_000),
            };
          },
        });
        const signer = S3Signing.make(storage);
        const ref = { storeId: storage.storeId, key: "private" };
        const grant = yield* signer.sign({ ref, method: "GET", expiresInSeconds: 600 });
        assert.strictEqual(
          new URL(Redacted.value(grant.url)).searchParams.get("X-Amz-Expires"),
          "30",
        );
        assert.strictEqual(
          DateTime.toEpochMillis(grant.expiresAt) - DateTime.toEpochMillis(now),
          30_000,
        );
        yield* signer.sign({ ref, method: "HEAD", expiresInSeconds: 600 });
        assert.isAtLeast(refreshes, 2);
        assert.notInclude(JSON.stringify(grant), token);
      }),
    ),
);

it.effect("expired credentials fail before a grant is returned", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const storage = yield* S3Storage.make({
        bucket: "signing-tests",
        region: "us-east-1",
        credentials: {
          ...fixtureCredentials(),
          expiration: new Date(DateTime.toEpochMillis(now) - 1000),
        },
      });
      const result = yield* Effect.result(
        S3Signing.make(storage).sign({
          ref: { storeId: storage.storeId, key: "expired" },
          method: "GET",
          expiresInSeconds: 60,
        }),
      );
      assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "Expired");
    }),
  ),
);

it.effect(
  "part signing binds upload identity and SSE-C requirements stay inside redacted headers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const key = randomBytes(32).toString("base64");
        const storage = yield* S3Storage.make({
          bucket: "signing-tests",
          region: "us-east-1",
          credentials: fixtureCredentials(),
          encryption: {
            mode: "customer",
            key: Redacted.make(key),
            keyMD5: randomBytes(16).toString("base64"),
          },
        });
        const signer = S3Signing.make(storage);
        const grant = yield* signer.signPart({
          upload: {
            ref: { storeId: storage.storeId, key: "multipart" },
            uploadId: Redacted.make("upload-id"),
          },
          partNumber: 3,
          expiresInSeconds: 60,
        });
        const url = new URL(Redacted.value(grant.url));
        assert.strictEqual(url.searchParams.get("uploadId"), "upload-id");
        assert.strictEqual(url.searchParams.get("partNumber"), "3");
        assert.isDefined(
          Redacted.value(grant.headers)["x-amz-server-side-encryption-customer-key"],
        );
        assert.notInclude(JSON.stringify(grant), key);
        assert.notInclude(JSON.stringify(grant), "upload-id");
      }),
    ),
);

it.effect("POST policies bind the exact key, byte range, metadata and effective expiry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* S3Storage.make({
        bucket: "signing-tests",
        region: "us-east-1",
        credentials: fixtureCredentials(),
      });
      const grant = yield* S3Post.make(storage).create({
        key: "forms/image.png",
        expiresInSeconds: 60,
        minBytes: 1,
        maxBytes: 1024,
        contentType: "image/png",
        customMetadata: { purpose: "avatar" },
      });
      const fields = Redacted.value(grant.fields);
      const policy = JSON.parse(Buffer.from(fields.Policy ?? "", "base64").toString()) as {
        conditions: unknown[];
        expiration: string;
      };
      assert.includeDeepMembers(policy.conditions, [
        ["content-length-range", 1, 1024],
        { key: "forms/image.png" },
        { "Content-Type": "image/png" },
        { "x-amz-meta-purpose": "avatar" },
      ]);
      assert.strictEqual(
        policy.expiration,
        DateTime.formatIso(grant.expiresAt).replace(".000Z", "Z"),
      );
      assert.notInclude(JSON.stringify(grant), "Policy");
    }),
  ),
);

it.effect(
  "signing and public URL Layers require explicit composition without changing visibility",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const base = S3Storage.layer({
          bucket: "signing-tests",
          region: "us-east-1",
          credentials: fixtureCredentials(),
        });
        const signing = S3Signing.layer.pipe(Layer.provideMerge(base));
        const program = Effect.gen(function* () {
          const storage = yield* S3Storage;
          const signer = yield* StorageSigning;
          return yield* signer.sign({
            ref: { storeId: storage.storeId, key: "private" },
            method: "GET",
            expiresInSeconds: 60,
          });
        });
        assert.strictEqual((yield* program.pipe(Effect.provide(signing))).method, "GET");
        const urls = S3Storage.publicUrls("https://cdn.example.invalid/assets/").pipe(
          Layer.provideMerge(base),
        );
        yield* Effect.gen(function* () {
          const storage = yield* S3Storage;
          const resolver = yield* StoragePublicUrls;
          assert.strictEqual(
            yield* resolver.url({ storeId: storage.storeId, key: "images/a b" }),
            "https://cdn.example.invalid/assets/images/a%20b",
          );
          const unsafe = yield* Effect.result(
            resolver.url({ storeId: storage.storeId, key: "a/../b" }),
          );
          assert.isTrue(Result.isFailure(unsafe) && unsafe.failure.reason._tag === "Unsupported");
        }).pipe(Effect.provide(urls));
      }),
    ),
);
