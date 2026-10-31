import { createServer } from "node:http";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";

import { Storage } from "../../../src/core/storage.js";
import { SupabaseS3Storage } from "../../../src/providers/supabase/s3.js";

const jwt = (subject: string) =>
  `e30.${Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 60, sub: subject })).toString("base64url")}.signature`;

it.effect("uses explicit JWT S3 authority, endpoint paths and required-only checksums", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const requests: { path: string; headers: Record<string, unknown> }[] = [];
      let stored = Buffer.alloc(0);
      const parts = new Map<number, Buffer>();
      const server = yield* Effect.acquireRelease(
        Effect.promise(async () => {
          const instance = createServer(async (request, response) => {
            requests.push({ path: request.url ?? "", headers: request.headers });
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            const url = new URL(request.url ?? "/", "http://localhost");
            if (url.searchParams.has("uploads")) {
              response.writeHead(200, { "content-type": "application/xml" });
              response.end(
                "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>a</Key><UploadId>upload</UploadId></InitiateMultipartUploadResult>",
              );
              return;
            }
            if (url.searchParams.has("uploadId")) {
              if (request.method === "PUT") {
                const number = Number(url.searchParams.get("partNumber"));
                parts.set(number, Buffer.concat(chunks));
                response.writeHead(200, { etag: `"part-${number}"` });
                response.end();
                return;
              }
              if (request.method === "POST") {
                stored = Buffer.concat(
                  [...parts.entries()].toSorted(([a], [b]) => a - b).map(([, bytes]) => bytes),
                );
                response.writeHead(200, { "content-type": "application/xml" });
                response.end(
                  '<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>a</Key><ETag>"saved"</ETag></CompleteMultipartUploadResult>',
                );
                return;
              }
            }
            if (request.method === "PUT") stored = Buffer.concat(chunks);
            response.writeHead(200, {
              etag: '"saved"',
              "content-length":
                request.method === "GET" || request.method === "HEAD" ? String(stored.length) : "0",
              "content-type": "application/octet-stream",
            });
            response.end(request.method === "GET" ? stored : undefined);
          });
          await new Promise<void>((resolve) => instance.listen(0, "127.0.0.1", resolve));
          return instance;
        }),
        (instance) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                instance.closeAllConnections();
                instance.close(() => resolve());
              }),
          ),
      );
      const address = server.address();
      if (!address || typeof address === "string") return assert.fail("Missing fixture address");
      let token = jwt("user-a");
      const storage = yield* SupabaseS3Storage.make({
        endpoint: `http://127.0.0.1:${address.port}/storage/v1/s3`,
        bucket: "bucket",
        region: "local",
        storeId: "supabase-s3:test",
        auth: {
          mode: "user-session",
          projectRef: "project",
          anonKey: Redacted.make("anon"),
          token: () => Promise.resolve(Redacted.make(token)),
        },
      });
      yield* storage.put({ key: "a", body: new Uint8Array([1, 2]) });
      token = jwt("user-a-refreshed");
      assert.strictEqual((yield* storage.head({ key: "a" })).size, 2);
      assert.deepStrictEqual(
        Array.from(
          yield* Storage.getBytes({ key: "a", maxBytes: 8 }).pipe(
            Effect.provideService(Storage, storage),
          ),
        ),
        [1, 2],
      );
      assert.isTrue(
        requests.every((request) => request.path.startsWith("/storage/v1/s3/bucket/a")),
      );
      assert.strictEqual(requests.at(-1)?.headers["x-amz-security-token"], token);
      assert.isFalse(
        requests.some(
          (request) =>
            request.headers["x-amz-sdk-checksum-algorithm"] ||
            request.headers["x-amz-checksum-mode"],
        ),
      );
      const upload = yield* storage.multipart.initiate({ key: "a" });
      const part = yield* storage.multipart.uploadPart({
        upload,
        partNumber: 1,
        body: new Uint8Array([3, 4]),
        contentLength: 2,
      });
      yield* storage.multipart.complete({ upload, parts: [part] });
      assert.deepStrictEqual(Array.from(stored), [3, 4]);
      assert.isFalse(
        requests.some(
          (request) =>
            request.headers["x-amz-checksum-algorithm"] ||
            request.headers["x-amz-sdk-checksum-algorithm"],
        ),
      );
      const before = requests.length;
      const unsupported = yield* storage
        .put({ key: "a", body: new Uint8Array(), condition: { _tag: "CreateOnly" } })
        .pipe(Effect.result);
      assert.strictEqual(
        unsupported._tag === "Failure" && unsupported.failure.reason._tag,
        "Unsupported",
      );
      assert.strictEqual(requests.length, before);
    }),
  ),
);
