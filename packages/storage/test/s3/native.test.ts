import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result } from "effect";

import { S3Storage } from "../../src/s3.js";
import { drain, httpFixture, xml } from "./http-fixture.js";

it.effect(
  "multipart cursors bind uploads and queries and preserve native continuation markers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const paths: URL[] = [];
        const fixture = yield* httpFixture((incoming, response) => {
          const url = new URL(incoming.url ?? "", "http://fixture");
          paths.push(url);
          if (url.searchParams.has("uploads"))
            return xml(
              response,
              url.searchParams.has("key-marker")
                ? "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>"
                : "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><NextKeyMarker>p/a</NextKeyMarker><NextUploadIdMarker>next-upload</NextUploadIdMarker></ListMultipartUploadsResult>",
            );
          return xml(
            response,
            url.searchParams.has("part-number-marker")
              ? "<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>"
              : "<ListPartsResult><IsTruncated>true</IsTruncated><NextPartNumberMarker>1</NextPartNumberMarker></ListPartsResult>",
          );
        });
        const upload = {
          ref: { storeId: fixture.storage.storeId, key: "parts" },
          uploadId: Redacted.make("private-upload-id"),
        };
        const first = yield* fixture.storage.multipart.listParts({ upload });
        assert.isDefined(first.cursor);
        assert.notInclude(
          Buffer.from(first.cursor ?? "", "base64url").toString(),
          "private-upload-id",
        );
        const other = yield* Effect.result(
          fixture.storage.multipart.listParts({
            upload: { ...upload, uploadId: Redacted.make("other-upload") },
            cursor: first.cursor,
          }),
        );
        assert.isTrue(Result.isFailure(other) && other.failure.reason._tag === "InvalidInput");
        yield* fixture.storage.multipart.listParts({ upload, cursor: first.cursor });
        assert.strictEqual(paths.at(-1)?.searchParams.get("part-number-marker"), "1");
        const pending = yield* fixture.storage.multipart.listUploads({ prefix: "p/", limit: 1 });
        yield* fixture.storage.multipart.listUploads({
          prefix: "p/",
          limit: 1,
          cursor: pending.cursor,
        });
        assert.strictEqual(paths.at(-1)?.searchParams.get("key-marker"), "p/a");
        assert.strictEqual(paths.at(-1)?.searchParams.get("upload-id-marker"), "next-upload");
      }),
    ),
);

it.effect(
  "multipart lifecycle preserves checksum policy, part receipts, pagination and conditions",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const seen: Array<{ path: string; body: string; condition: string | undefined }> = [];
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then((bytes) => {
            const url = new URL(incoming.url ?? "", "http://fixture");
            const body = Buffer.from(bytes).toString();
            seen.push({
              path: url.search,
              body,
              condition: incoming.headers["if-none-match"] as string | undefined,
            });
            if (url.searchParams.has("uploads") && incoming.method === "POST")
              return xml(
                response,
                "<InitiateMultipartUploadResult><UploadId>native-upload</UploadId></InitiateMultipartUploadResult>",
              );
            if (incoming.method === "PUT") {
              response.writeHead(200, { etag: '"part"', "x-amz-checksum-crc32": "AAAAAA==" });
              return response.end();
            }
            if (incoming.method === "GET" && url.searchParams.has("uploads"))
              return xml(
                response,
                "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>multipart</Key><UploadId>native-upload</UploadId><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><ChecksumType>COMPOSITE</ChecksumType></Upload></ListMultipartUploadsResult>",
              );
            if (incoming.method === "GET")
              return xml(
                response,
                '<ListPartsResult><IsTruncated>false</IsTruncated><ChecksumType>COMPOSITE</ChecksumType><Part><PartNumber>1</PartNumber><ETag>"part"</ETag><Size>0</Size><ChecksumCRC32>AAAAAA==</ChecksumCRC32></Part></ListPartsResult>',
              );
            if (incoming.method === "DELETE") {
              response.writeHead(204);
              return response.end();
            }
            return xml(
              response,
              '<CompleteMultipartUploadResult><ETag>"complete"</ETag><ChecksumCRC32>AAAAAA==-1</ChecksumCRC32><ChecksumType>COMPOSITE</ChecksumType></CompleteMultipartUploadResult>',
            );
          });
        });
        const upload = yield* fixture.storage.multipart.initiate({
          key: "multipart",
          checksum: { ChecksumAlgorithm: "CRC32", ChecksumType: "COMPOSITE" },
        });
        assert.strictEqual(upload.checksumAlgorithm, "CRC32");
        const part = yield* fixture.storage.multipart.uploadPart({
          upload,
          partNumber: 1,
          body: new Uint8Array(),
        });
        assert.strictEqual(part.etag, '"part"');
        assert.strictEqual(
          (yield* fixture.storage.multipart.listParts({ upload })).parts[0]?.checksums?.[0]?.scope,
          "full-object",
        );
        const inventory = yield* fixture.storage.multipart.listUploads({});
        assert.strictEqual(inventory.uploads.length, 1);
        assert.strictEqual(inventory.uploads[0]?.checksumAlgorithm, "CRC32");
        assert.strictEqual(inventory.uploads[0]?.checksumType, "COMPOSITE");
        const complete = yield* fixture.storage.multipart.complete({
          upload,
          parts: [part],
          condition: { _tag: "CreateOnly" },
        });
        assert.strictEqual(complete.etag, '"complete"');
        assert.isTrue(
          seen.some((entry) => entry.condition === "*" && entry.body.includes("ChecksumCRC32")),
        );
        yield* fixture.storage.multipart.abort(upload);
        const invalid = yield* Effect.result(
          fixture.storage.multipart.uploadPart({
            upload,
            partNumber: 10001,
            body: new Uint8Array(),
          }),
        );
        assert.isTrue(Result.isFailure(invalid) && invalid.failure.reason._tag === "InvalidInput");
        assert.notInclude(JSON.stringify(upload), "native-upload");
      }),
    ),
);

it.effect(
  "multipart completion orders parts and rejects duplicates and undersized nonfinal parts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let body = "";
        let sends = 0;
        const fixture = yield* httpFixture((incoming, response) => {
          sends++;
          void drain(incoming).then((bytes) => {
            body = Buffer.from(bytes).toString();
            return xml(
              response,
              '<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>',
            );
          });
        });
        const upload = {
          ref: { storeId: fixture.storage.storeId, key: "key" },
          uploadId: Redacted.make("test-session"),
        };
        yield* fixture.storage.multipart.complete({
          upload,
          parts: [
            { partNumber: 2, etag: '"b"' },
            { partNumber: 1, etag: '"a"', size: 5 * 1024 * 1024 },
          ],
        });
        assert.isBelow(
          body.indexOf("<PartNumber>1</PartNumber>"),
          body.indexOf("<PartNumber>2</PartNumber>"),
        );
        for (const parts of [
          [
            { partNumber: 1, etag: '"a"' },
            { partNumber: 1, etag: '"b"' },
          ],
          [
            { partNumber: 1, etag: '"a"', size: 1 },
            { partNumber: 2, etag: '"b"' },
          ],
        ]) {
          const result = yield* Effect.result(
            fixture.storage.multipart.complete({ upload, parts }),
          );
          assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "InvalidInput");
        }
        assert.strictEqual(sends, 1);
      }),
    ),
);

it.effect(
  "copy maps source/destination conditions, version encoding, metadata and cross-bucket authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let seen: Record<string, string | string[] | undefined> = {};
        const fixture = yield* httpFixture((incoming, response) => {
          seen = incoming.headers;
          return xml(
            response,
            '<CopyObjectResult><ETag>"copy"</ETag><ChecksumSHA256>checksum</ChecksumSHA256></CopyObjectResult>',
          );
        });
        const source = yield* S3Storage.make({
          ...fixture.config,
          bucket: "other-bucket",
          storeId: "source",
        });
        const receipt = yield* fixture.storage.copyObject(
          {
            source: {
              storeId: source.storeId,
              key: "a//%2F",
              version: { _tag: "S3Version", id: "v/1" },
            },
            destinationKey: "destination",
            sourceIfMatch: '"old"',
            condition: { _tag: "CreateOnly" },
            metadataDirective: "REPLACE",
            headers: { contentType: "text/plain" },
          },
          source,
        );
        assert.strictEqual(seen["x-amz-copy-source"], "other-bucket/a//%252F?versionId=v%2F1");
        assert.strictEqual(seen["x-amz-copy-source-if-match"], '"old"');
        assert.strictEqual(seen["if-none-match"], "*");
        assert.strictEqual(seen["x-amz-metadata-directive"], "REPLACE");
        assert.strictEqual(receipt.etag, '"copy"');
        assert.strictEqual(receipt.checksums?.[0]?.algorithm, "sha256");
      }),
    ),
);

it.effect(
  "SDK decodes embedded HTTP 200 copy and complete failures without claiming rejection",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then(() =>
            xml(response, "<Error><Code>InternalError</Code><Message>SECRET</Message></Error>"),
          );
        });
        const copy = yield* Effect.result(
          fixture.storage.copyObject({
            source: { storeId: fixture.storage.storeId, key: "source" },
            destinationKey: "copy",
          }),
        );
        assert.isTrue(Result.isFailure(copy) && copy.failure.mutationOutcome === "unknown");
        const complete = yield* Effect.result(
          fixture.storage.multipart.complete({
            upload: {
              ref: { storeId: fixture.storage.storeId, key: "complete" },
              uploadId: Redacted.make("native-upload"),
            },
            parts: [{ partNumber: 1, etag: '"part"' }],
          }),
        );
        assert.isTrue(Result.isFailure(complete) && complete.failure.mutationOutcome === "unknown");
        assert.notInclude(JSON.stringify([copy, complete]), "SECRET");
      }),
    ),
);

it.effect(
  "native batch delete retains per-key failures and missing receipts without hiding success",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then(() =>
            xml(
              response,
              "<DeleteResult><Deleted><Key>a</Key></Deleted><Error><Key>b</Key><Code>AccessDenied</Code><Message>SECRET</Message></Error></DeleteResult>",
            ),
          );
        });
        const outcomes = yield* fixture.storage.deleteObjects([
          { key: "a" },
          { key: "b" },
          { key: "c" },
        ]);
        assert.isTrue(outcomes[0] !== undefined && Result.isSuccess(outcomes[0].result));
        const b = outcomes[1]?.result;
        const c = outcomes[2]?.result;
        assert.isTrue(
          b !== undefined && Result.isFailure(b) && b.failure.reason._tag === "AccessDenied",
        );
        assert.isTrue(
          c !== undefined && Result.isFailure(c) && c.failure.mutationOutcome === "unknown",
        );
        assert.notInclude(JSON.stringify(outcomes), "SECRET");
      }),
    ),
);

it.effect("native batch response loss reports unknown independently for every requested key", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture((incoming) => {
        void drain(incoming).then(() => incoming.socket.destroy());
      });
      const outcomes = yield* fixture.storage.deleteObjects([{ key: "a" }, { key: "b" }]);
      assert.isTrue(
        outcomes.every(
          (outcome) =>
            Result.isFailure(outcome.result) &&
            outcome.result.failure.mutationOutcome === "unknown",
        ),
      );
    }),
  ),
);
