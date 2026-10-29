import { crc32 } from "node:zlib";

import { assert, it } from "@effect/vitest";
import { Effect } from "effect";

import { s3TransferEndpoint } from "../../src/s3/transfers.js";
import { mapS3Error } from "../../src/s3/transport.js";
import { Transfers, UploadSource } from "../../src/transfers.js";
import { sha256 } from "../../src/transfers/node.js";
import { drain, httpFixture, xml } from "../s3/http-fixture.js";

it.live(
  "official SDK multipart retries share one budget and reconcile a lost completion response",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let operationId = "";
        let committed = false;
        let firstAttempts = 0;
        let requests = 0;
        let aborts = 0;
        const size = 5 * 1024 * 1024 + 1;
        const fixture = yield* httpFixture((incoming, response) => {
          requests++;
          const query = new URL(incoming.url ?? "/", "http://fixture").searchParams;
          if (incoming.method === "HEAD") {
            response.writeHead(committed ? 200 : 404, {
              "content-length": String(size),
              etag: '"final"',
              "x-amz-meta-effect-storage-operation": operationId,
              "x-amz-version-id": "version-1",
            });
            response.end();
          } else if (incoming.method === "POST" && query.has("uploads")) {
            operationId = String(incoming.headers["x-amz-meta-effect-storage-operation"]);
            xml(
              response,
              "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>",
            );
          } else if (incoming.method === "PUT") {
            const part = query.get("partNumber");
            void drain(incoming).then(
              (body) => {
                if (part === "1" && firstAttempts++ === 0) {
                  response.setHeader("retry-after", "0");
                  xml(response, "<Error><Code>SlowDown</Code></Error>", 503);
                  return;
                }
                const checksum = Buffer.alloc(4);
                checksum.writeUInt32BE(crc32(body));
                response.writeHead(200, {
                  etag: `"part-${part}"`,
                  "x-amz-checksum-crc32": checksum.toString("base64"),
                });
                return response.end();
              },
              () => response.destroy(),
            );
          } else if (incoming.method === "POST") {
            void drain(incoming).then(
              () => {
                committed = true;
                return response.destroy();
              },
              () => response.destroy(),
            );
          } else if (incoming.method === "DELETE") {
            aborts++;
            response.writeHead(204).end();
          } else response.writeHead(500).end();
        });
        const transfers = yield* Transfers.make(s3TransferEndpoint(fixture.storage), {
          digest: sha256,
          partBytes: 5 * 1024 * 1024,
          partConcurrency: 2,
          memoryBudgetBytes: 24 * 1024 * 1024,
          maxBytes: size,
          maxAttempts: 10,
          retryBaseMillis: 1,
        });
        const receipt = yield* transfers.upload({
          key: "managed",
          source: UploadSource.bytes(new Uint8Array(size)),
        });
        assert.strictEqual(receipt.progress.bytesAcknowledged, size);
        assert.strictEqual(receipt.progress.attempts, requests);
        assert.strictEqual(requests, 6);
        assert.strictEqual(firstAttempts, 2);
        assert.strictEqual(aborts, 0);
        assert.strictEqual((yield* transfers.resources).reservedBytes, 0);
      }),
    ),
);

it.effect("maps safe throttle Retry-After seconds and dates with a supplied clock", () =>
  Effect.sync(() => {
    for (const header of ["2", "Thu, 29 Oct 2026 00:00:02 GMT"]) {
      const error = mapS3Error(
        {
          name: "SlowDown",
          $metadata: { httpStatusCode: 503 },
          $response: { headers: { "retry-after": header } },
        },
        "part",
        true,
        Date.parse("2026-10-29T00:00:00Z"),
      );
      assert.deepEqual(error.reason, { _tag: "Throttled", retryAfterMillis: 2000 });
      assert.strictEqual(error.mutationOutcome, "rejected");
    }
  }),
);
