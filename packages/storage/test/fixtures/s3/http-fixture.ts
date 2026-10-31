import { randomBytes } from "node:crypto";
import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";

import { Effect } from "effect";

import { S3Client } from "@aws-sdk/client-s3";

import { S3Storage } from "../../../src/providers/s3/index.js";
import type { S3Config } from "../../../src/providers/s3/index.js";

export const fixtureCredentials = () => ({
  accessKeyId: randomBytes(10).toString("hex"),
  secretAccessKey: randomBytes(24).toString("hex"),
});

export const httpFixture = Effect.fnUntraced(function* (
  handle: (incoming: IncomingMessage, response: ServerResponse) => void,
) {
  const server = createServer(handle);
  yield* Effect.acquireRelease(
    Effect.promise(
      () =>
        new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(0, "127.0.0.1", resolve);
        }),
    ),
    () =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            server.closeAllConnections();
            server.close(() => resolve());
          }),
      ),
  );
  const address = server.address();
  if (!address || typeof address === "string")
    return yield* Effect.die(new Error("HTTP fixture has no TCP address"));
  const endpoint = `http://127.0.0.1:${String(address.port)}`;
  const config: S3Config = {
    bucket: "storage-tests",
    region: "us-east-1",
    endpoint,
    forcePathStyle: true,
    credentials: fixtureCredentials(),
  };
  return { endpoint, config, storage: yield* S3Storage.make(config) };
});

export async function drain(incoming: IncomingMessage): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of incoming) chunks.push(chunk);
  return Buffer.concat(chunks);
}

export function xml(response: ServerResponse, body: string, status = 200) {
  response.writeHead(status, { "content-type": "application/xml" });
  response.end(body);
}

export function client(config: S3Config) {
  return new S3Client({
    region: config.region,
    endpoint: config.endpoint ?? "http://127.0.0.1:1",
    forcePathStyle: true,
    credentials: config.credentials ?? fixtureCredentials(),
    maxAttempts: 1,
    requestChecksumCalculation: "WHEN_SUPPORTED",
    responseChecksumValidation: "WHEN_SUPPORTED",
  });
}
