import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result, Stream } from "effect";

import { R2Storage } from "../../../src/providers/r2/index.js";
import { S3Post } from "../../../src/providers/s3/post.js";
import { S3Signing } from "../../../src/providers/s3/signing.js";
import { profileFixtures, settings } from "../../fixtures/s3-compatible/profiles.js";
import { drain, httpFixture, xml } from "../../fixtures/s3/http-fixture.js";

for (const profile of profileFixtures) {
  it.effect(
    `${profile.name}: scoped bytes, metadata, signing region and profile-safe failures`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let saved: Uint8Array = new Uint8Array();
          let seen = 0;
          const fixture = yield* httpFixture((incoming, response) => {
            seen++;
            if (incoming.method === "PUT") {
              void drain(incoming).then((bytes) => {
                saved = bytes;
                response.writeHead(200, { etag: '"saved"', "x-amz-version-id": "saved-version" });
                return response.end();
              });
            } else if (incoming.method === "DELETE") {
              response.writeHead(204);
              response.end();
            } else if (incoming.method === "GET" && incoming.url?.includes("list-type"))
              xml(
                response,
                '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>object</Key><Size>3</Size><ETag>"saved"</ETag></Contents></ListBucketResult>',
              );
            else {
              response.writeHead(200, {
                "content-length": String(saved.length),
                "content-type": "application/octet-stream",
                "x-amz-meta-label": "same-response",
                etag: '"saved"',
                "x-amz-version-id": "saved-version",
              });
              response.end(incoming.method === "HEAD" ? undefined : saved);
            }
          });
          const storage = yield* profile.make(fixture.endpoint);
          assert.strictEqual(storage.provider, profile.name);
          const receipt = yield* storage.put({ key: "object", body: new Uint8Array([1, 2, 3]) });
          assert.strictEqual(receipt.size, 3);
          const expectedVersion =
            storage.capabilities.versionedReads === "none"
              ? undefined
              : { _tag: "S3Version", id: "saved-version" };
          assert.deepStrictEqual(receipt.ref.version, expectedVersion);

          const result = yield* storage.get({ ref: receipt.ref });
          assert.strictEqual(result._tag, "Object");
          if (result._tag !== "Object") return;
          assert.strictEqual(result.metadata.customMetadata?.label, "same-response");
          assert.deepStrictEqual(result.metadata.ref.version, expectedVersion);
          assert.deepStrictEqual(
            Array.from(yield* Stream.runCollect(result.body)).flatMap((b) => Array.from(b)),
            [1, 2, 3],
          );
          const observed = yield* storage.head({ ref: receipt.ref });
          assert.strictEqual(observed.size, 3);
          assert.deepStrictEqual(observed.ref.version, expectedVersion);
          assert.strictEqual((yield* storage.listPage()).entries.length, 1);
          const sign = yield* S3Signing.make(storage).sign({
            ref: receipt.ref,
            method: "GET",
            expiresInSeconds: 60,
          });
          const url = new URL(Redacted.value(sign.url));
          assert.include(url.searchParams.get("X-Amz-Credential"), `/${storage.config.region}/s3/`);
          assert.strictEqual(
            new URL(Redacted.value(sign.url)).host,
            new URL(fixture.endpoint).host,
          );
          const before = seen;
          const denied = yield* Effect.result(
            storage.delete({ key: "object", condition: { _tag: "IfMatch", etag: '"saved"' } }),
          );
          assert.isTrue(Result.isFailure(denied));
          if (Result.isFailure(denied)) {
            assert.strictEqual(denied.failure.reason._tag, "Unsupported");
            assert.strictEqual(denied.failure.provider, profile.name);
          }
          assert.strictEqual(seen, before);
          if (!storage.profile?.post) {
            const grant = yield* Effect.result(
              S3Post.make(storage).create({ key: "object", expiresInSeconds: 30, maxBytes: 4 }),
            );
            assert.isTrue(Result.isFailure(grant) && grant.failure.reason._tag === "Unsupported");
          }
          yield* storage.delete({ ref: receipt.ref });
        }),
      ),
  );

  it.effect(`${profile.name}: multipart lifecycle uses bounded native inventory and receipts`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture((incoming, response) => {
          const url = new URL(incoming.url ?? "/", "http://localhost");
          if (incoming.method === "POST" && url.searchParams.has("uploads"))
            xml(
              response,
              "<InitiateMultipartUploadResult><UploadId>owned-upload</UploadId><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType></InitiateMultipartUploadResult>",
            );
          else if (incoming.method === "PUT")
            void drain(incoming).then(() => {
              response.writeHead(200, { etag: '"part"', "x-amz-checksum-crc32": "pQDfGw==" });
              return response.end();
            });
          else if (incoming.method === "GET" && url.searchParams.has("uploadId"))
            xml(
              response,
              '<ListPartsResult><IsTruncated>false</IsTruncated><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType><Part><PartNumber>1</PartNumber><ETag>"part"</ETag><Size>1</Size><ChecksumCRC32>pQDfGw==</ChecksumCRC32></Part></ListPartsResult>',
            );
          else if (incoming.method === "POST")
            void drain(incoming).then(() => {
              response.setHeader("x-amz-version-id", "completed-version");
              return xml(
                response,
                '<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>',
              );
            });
          else if (incoming.method === "DELETE") {
            response.writeHead(204);
            response.end();
          } else
            xml(
              response,
              "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>",
            );
        });
        const storage = yield* profile.make(fixture.endpoint);
        const upload = yield* storage.multipart.initiate({ key: "multipart" });
        const part = yield* storage.multipart.uploadPart({
          upload,
          partNumber: 1,
          body: new Uint8Array([1]),
        });
        assert.strictEqual(part.etag, '"part"');
        const inventory = yield* storage.multipart.listParts({ upload });
        assert.strictEqual(inventory.parts.length, 1);
        if (!storage.profile?.checksums) {
          assert.isUndefined(upload.checksumAlgorithm);
          assert.isUndefined(upload.checksumType);
          assert.isUndefined(part.checksums);
          assert.isUndefined(inventory.parts[0]?.checksums);
          assert.isUndefined(inventory.checksumAlgorithm);
        } else {
          assert.strictEqual(upload.checksumAlgorithm, "CRC32");
          assert.strictEqual(part.checksums?.[0]?.algorithm, "crc32");
        }
        assert.strictEqual((yield* storage.multipart.listUploads({})).uploads.length, 0);
        const completed = yield* storage.multipart.complete({ upload, parts: [part] });
        assert.strictEqual(completed.etag, '"complete"');
        assert.deepStrictEqual(
          completed.ref.version,
          storage.capabilities.versionedReads === "none"
            ? undefined
            : { _tag: "S3Version", id: "completed-version" },
        );
        yield* storage.multipart.abort(upload);
      }),
    ),
  );
}

it.effect("R2 copy receipts remain usable when the service returns an upload version", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture((incoming, response) => {
        assert.strictEqual(incoming.method, "PUT");
        response.setHeader("x-amz-version-id", "copy-upload-version");
        xml(response, '<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>');
      });
      const storage = yield* R2Storage.make({
        ...settings(fixture.endpoint),
        accountId: "a".repeat(32),
      });

      const copied = yield* storage.copyObject({
        source: { storeId: storage.storeId, key: "source" },
        destinationKey: "copy",
      });

      assert.strictEqual(copied.ref.key, "copy");
      assert.isUndefined(copied.ref.version);
      assert.strictEqual(copied.etag, '"copied"');
    }),
  ),
);

it.effect(
  "documented create-only profiles send native competing conditions and preserve rejection identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const profile of profileFixtures.filter((p) =>
          ["r2", "tigris", "scaleway"].includes(p.name),
        )) {
          let committed = false;
          const fixture = yield* httpFixture((incoming, response) => {
            assert.strictEqual(incoming.headers["if-none-match"], "*");
            void drain(incoming).then(() => {
              if (committed) xml(response, "<Error><Code>PreconditionFailed</Code></Error>", 412);
              else {
                committed = true;
                response.writeHead(200, { etag: '"winner"' });
                response.end();
              }
              return undefined;
            });
          });
          const storage = yield* profile.make(fixture.endpoint);
          const results = yield* Effect.forEach(
            [0, 1],
            () =>
              Effect.result(
                storage.put({
                  key: "race",
                  body: new Uint8Array(),
                  condition: { _tag: "CreateOnly" },
                }),
              ),
            { concurrency: 2 },
          );
          assert.strictEqual(results.filter(Result.isSuccess).length, 1);
          const failure = results.find(Result.isFailure);
          assert.strictEqual(failure?.failure.reason._tag, "PreconditionFailed");
          assert.strictEqual(failure?.failure.provider, profile.name);
        }
      }),
    ),
);

it.effect("unsupported conditions reject before materializing a one-shot source", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let reads = 0;
      for (const profile of profileFixtures.filter(
        (p) => !["r2", "tigris", "scaleway"].includes(p.name),
      )) {
        const storage = yield* profile.make();
        const result = yield* Effect.result(
          storage.putObject({
            key: "blocked",
            condition: { _tag: "CreateOnly" },
            body: Stream.fromEffect(
              Effect.sync(() => {
                reads++;
                return new Uint8Array();
              }),
            ),
          }),
        );
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "Unsupported");
      }
      assert.strictEqual(reads, 0);
    }),
  ),
);
