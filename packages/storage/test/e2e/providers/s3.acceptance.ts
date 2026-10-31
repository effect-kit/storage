import { crc32 } from "node:zlib";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result, Schema, Stream } from "effect";

import { GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

import { Storage } from "../../../src/index.js";
import { VersionRef } from "../../../src/lifecycle/index.js";
import { S3Storage } from "../../../src/providers/s3/index.js";
import { S3Versions, S3Tags } from "../../../src/providers/s3/lifecycle.js";
import { S3Post } from "../../../src/providers/s3/post.js";
import { S3Signing } from "../../../src/providers/s3/signing.js";
import { runStorageConformance } from "../../../src/testing/index.js";
import { conformanceFixture, environment, s3Fixture } from "../../fixtures/s3/acceptance.js";

it.live("version and marker deletion preserve history and version-specific object tags", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { storage, prefix } = yield* s3Fixture();
      const key = `${prefix}advanced-history`;
      const initial = yield* storage.put({ key, body: new Uint8Array([1, 2, 3]) });
      const original = yield* Schema.decodeUnknownEffect(VersionRef)(initial.ref);
      const versions = S3Versions.make(storage);
      const tags = S3Tags.make(storage);
      yield* tags.set({ ref: original, tags: { owner: "acceptance" } });
      yield* storage.put({ key, body: new Uint8Array([4]) });
      assert.deepStrictEqual(yield* tags.get({ ref: original }), { owner: "acceptance" });
      yield* storage.delete({ key });
      const entries = (yield* versions.listPage({ prefix: key })).entries;
      const marker = entries.find((entry) => entry._tag === "DeleteMarker");
      if (!marker || marker._tag !== "DeleteMarker") return assert.fail("Missing delete marker");
      yield* versions.removeDeleteMarker({ ref: marker.ref });
      assert.deepStrictEqual(
        yield* Storage.getBytes({ key, maxBytes: 4 }).pipe(Effect.provideService(Storage, storage)),
        new Uint8Array([4]),
      );
      yield* versions.deleteVersion({ ref: original });
      assert.deepStrictEqual(
        yield* Storage.getBytes({ key, maxBytes: 4 }).pipe(Effect.provideService(Storage, storage)),
        new Uint8Array([4]),
      );
    }),
  ),
);

it.live("public portable conformance against the selected provider", () =>
  Effect.gen(function* () {
    const results = yield* runStorageConformance(conformanceFixture);
    assert.isTrue(results.every((result) => result.status === "passed"));
  }),
);

it.live("native streaming, pinned versions, copy and version-specific deletion", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { storage, prefix } = yield* s3Fixture();
      const key = `${prefix}stream`;
      const original = yield* storage.put({
        key,
        contentLength: 3,
        body: Stream.make(new Uint8Array([1]), new Uint8Array([2, 3])),
      });
      assert.isDefined(original.ref.version);
      yield* storage.put({ key, body: new Uint8Array([4]) });
      assert.deepStrictEqual(
        yield* Storage.getBytes({ ref: original.ref, maxBytes: 3 }).pipe(
          Effect.provideService(Storage, storage),
        ),
        new Uint8Array([1, 2, 3]),
      );
      const copied = yield* storage.copyObject({
        source: original.ref,
        destinationKey: `${prefix}copy`,
        sourceIfMatch: original.etag,
      });
      assert.isDefined(copied.etag);
      yield* storage.delete({ ref: original.ref });
      assert.strictEqual((yield* storage.head({ key })).size, 1);
    }),
  ),
);

it.live("multipart inventory, part copy, checksum receipts and conditional completion", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { storage, prefix } = yield* s3Fixture();
      const source = yield* storage.put({
        key: `${prefix}source`,
        body: new Uint8Array(5 * 1024 * 1024),
      });
      const upload = yield* storage.multipart.initiate({
        key: `${prefix}multipart`,
        checksum: { ChecksumAlgorithm: "CRC32", ChecksumType: "COMPOSITE" },
      });
      const first = yield* storage.copyPart({
        upload,
        partNumber: 1,
        source: source.ref,
        sourceIfMatch: source.etag,
      });
      const second = yield* storage.multipart.uploadPart({
        upload,
        partNumber: 2,
        body: new Uint8Array([1]),
      });
      assert.strictEqual((yield* storage.multipart.listParts({ upload })).parts.length, 2);
      assert.isTrue(
        (yield* storage.multipart.listUploads({ prefix })).uploads.some(
          (entry) => Redacted.value(entry.uploadId) === Redacted.value(upload.uploadId),
        ),
      );
      const receipt = yield* storage.multipart.complete({
        upload,
        parts: [second, first],
        condition: { _tag: "CreateOnly" },
      });
      assert.isDefined(receipt.ref.version);
      assert.strictEqual((yield* storage.head({ ref: receipt.ref })).size, 5 * 1024 * 1024 + 1);
    }),
  ),
);

it.live("signed PUT/GET/HEAD/DELETE and part grants are accepted with their required headers", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { storage, prefix } = yield* s3Fixture();
      const signing = S3Signing.make(storage);
      const ref = { storeId: storage.storeId, key: `${prefix}signed` };
      const put = yield* signing.sign({
        ref,
        method: "PUT",
        expiresInSeconds: 60,
        headers: { contentType: "text/plain" },
        condition: { _tag: "CreateOnly" },
      });
      const written = yield* Effect.promise(() =>
        fetch(Redacted.value(put.url), {
          method: "PUT",
          headers: Redacted.value(put.headers),
          body: "signed",
        }),
      );
      assert.strictEqual(written.status, 200);
      yield* Effect.promise(() => written.arrayBuffer());
      const get = yield* signing.sign({ ref, method: "GET", expiresInSeconds: 60 });
      const response = yield* Effect.promise(() =>
        fetch(Redacted.value(get.url), { headers: Redacted.value(get.headers) }),
      );
      assert.strictEqual(yield* Effect.promise(() => response.text()), "signed");
      for (const method of ["HEAD", "DELETE"] as const) {
        const grant = yield* signing.sign({ ref, method, expiresInSeconds: 60 });
        const result = yield* Effect.promise(() =>
          fetch(Redacted.value(grant.url), { method, headers: Redacted.value(grant.headers) }),
        );
        assert.isTrue(result.ok);
        yield* Effect.promise(() => result.arrayBuffer());
      }
      const upload = yield* storage.multipart.initiate({
        key: `${prefix}signed-part`,
        checksum: { ChecksumAlgorithm: "CRC32" },
      });
      const bytes = new Uint8Array([1, 2, 3]);
      const digest = Buffer.alloc(4);
      digest.writeUInt32BE(crc32(bytes));
      const grant = yield* signing.signPart({
        upload,
        partNumber: 1,
        expiresInSeconds: 60,
        checksum: { ChecksumCRC32: digest.toString("base64") },
      });
      const part = yield* Effect.promise(() =>
        fetch(Redacted.value(grant.url), {
          method: "PUT",
          headers: Redacted.value(grant.headers),
          body: bytes,
        }),
      );
      assert.strictEqual(part.status, 200);
      yield* Effect.promise(() => part.arrayBuffer());
      assert.strictEqual((yield* storage.multipart.listParts({ upload })).parts.length, 1);
      yield* storage.multipart.abort(upload);
    }),
  ),
);

it.live("signed headers, expired grants and checksum corruption are rejected", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { storage, prefix } = yield* s3Fixture();
      const ref = { storeId: storage.storeId, key: `${prefix}invalid-grant` };
      const put = yield* S3Signing.make(storage).sign({
        ref,
        method: "PUT",
        expiresInSeconds: 60,
        headers: { contentType: "text/plain" },
      });
      const wrong = yield* Effect.promise(() =>
        fetch(Redacted.value(put.url), {
          method: "PUT",
          headers: { ...Redacted.value(put.headers), "content-type": "image/png" },
          body: "wrong",
        }),
      );
      assert.strictEqual(wrong.status, 403);
      yield* Effect.promise(() => wrong.arrayBuffer());
      const expired = yield* Effect.promise(() =>
        getSignedUrl(
          storage.client,
          new GetObjectCommand({ Bucket: storage.config.bucket, Key: ref.key }),
          { expiresIn: 1, signingDate: new Date(Date.now() - 120_000) },
        ),
      );
      const rejected = yield* Effect.promise(() => fetch(expired));
      assert.strictEqual(rejected.status, 403);
      yield* Effect.promise(() => rejected.arrayBuffer());
      const checksum = yield* Effect.result(
        storage.putObject(
          { key: `${prefix}checksum`, body: new Uint8Array([1, 2, 3]) },
          { ChecksumCRC32: "AAAAAA==" },
        ),
      );
      assert.isTrue(
        Result.isFailure(checksum) && checksum.failure.reason._tag === "IntegrityMismatch",
      );
    }),
  ),
);

it.live("POST forms enforce exact content fields and byte constraints", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { storage, prefix } = yield* s3Fixture();
      const grant = yield* S3Post.make(storage).create({
        key: `${prefix}post`,
        expiresInSeconds: 60,
        minBytes: 1,
        maxBytes: 16,
        contentType: "text/plain",
      });
      const form = new FormData();
      for (const [key, value] of Object.entries(Redacted.value(grant.fields)))
        form.append(key, value);
      form.append("file", new Blob(["posted"], { type: "text/plain" }));
      const response = yield* Effect.promise(() =>
        fetch(Redacted.value(grant.url), { method: "POST", body: form }),
      );
      assert.isTrue(response.ok);
      yield* Effect.promise(() => response.arrayBuffer());
      assert.strictEqual((yield* storage.head({ ref: grant.ref })).size, 6);
    }),
  ),
);

it.live.skipIf(environment.mode !== "aws")(
  "AWS expected-owner denial and wrong-region behavior",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { storage, prefix } = yield* s3Fixture();
        const key = `${prefix}authority`;
        yield* storage.put({ key, body: new Uint8Array() });
        const denied = yield* S3Storage.make({
          bucket: storage.config.bucket,
          region: storage.config.region,
          expectedBucketOwner: "000000000000",
        });
        const result = yield* Effect.result(denied.head({ key }));
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "AccessDenied");
        const wrong = yield* S3Storage.make({
          bucket: storage.config.bucket,
          region: storage.config.region === "us-east-1" ? "us-west-2" : "us-east-1",
        });
        const region = yield* Effect.result(wrong.head({ key }));
        assert.isTrue(Result.isFailure(region) && region.failure.reason._tag === "ProviderFailure");
      }),
    ),
);
