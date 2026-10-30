import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { createServer } from "node:http";

import { Effect, Redacted, Stream } from "effect";

import { Storage } from "@effect-kit/storage";
import { S3Storage } from "@effect-kit/storage/s3";
import { S3Post } from "@effect-kit/storage/s3/post";
import { S3Signing } from "@effect-kit/storage/s3/signing";

const server = createServer((request, response) => {
  if (request.method === "PUT") {
    request.resume();
    request.on("end", () => {
      response.writeHead(200, { etag: '"packed"', "x-amz-version-id": "packed-version" });
      response.end();
    });
  } else {
    response.writeHead(200, { "content-length": "3", etag: '"packed"' });
    response.end("sdk");
  }
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
try {
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* S3Storage.make({
          bucket: "artifact-test",
          region: "us-east-1",
          endpoint: `http://127.0.0.1:${address.port}`,
          forcePathStyle: true,
          credentials: {
            accessKeyId: randomBytes(12).toString("hex"),
            secretAccessKey: randomBytes(24).toString("hex"),
          },
        });
        const receipt = yield* storage.put({
          key: "packed",
          body: Stream.make(new Uint8Array([1]), new Uint8Array([2])),
          contentLength: 2,
        });
        assert.equal(receipt.ref.version?.id, "packed-version");
        assert.equal(
          yield* Storage.getText({ key: "packed", maxBytes: 3 }).pipe(
            Effect.provideService(Storage, storage),
          ),
          "sdk",
        );
        const grant = yield* S3Signing.make(storage).sign({
          ref: receipt.ref,
          method: "GET",
          expiresInSeconds: 60,
        });
        assert.ok(Redacted.value(grant.url).includes("X-Amz-Signature"));
        const post = yield* S3Post.make(storage).create({
          key: "form",
          expiresInSeconds: 60,
          maxBytes: 100,
        });
        assert.ok(Redacted.value(post.fields).Policy);
      }),
    ),
  );
} finally {
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}

assert(Object.keys(await import("@effect-kit/storage/s3/uploads")).length > 0);
