// Run after pnpm build: node packages/storage/examples/authorized-upload.mjs
// This self-contained demo uses ephemeral memory. Applications supply durable KVS,
// a stable encryption key, authentication and idempotent database publication.
import { randomBytes } from "node:crypto";
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Context, Effect, Layer, Redacted } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { KeyValueStore } from "effect/persistence";

import { UploadClient } from "../dist/client.js";
import { StorageHttp } from "../dist/http.js";
import { aes256Gcm } from "../dist/node.js";
import { MemoryStorage } from "../dist/testing.js";
import { Uploads, UploadStore, UploadGrantDriver, failUpload } from "../dist/uploads.js";

const actor = { subject: "demo-user", tenantId: "demo-tenant" };
const state = UploadStore.layer({
  namespace: "demo",
  ownership: "exclusive",
  protection: aes256Gcm(Redacted.make(randomBytes(32))),
}).pipe(Layer.provide(KeyValueStore.layerMemory));
const publications = new Map();
const uploads = Uploads.layer({
  quarantine: "private",
  authorize: ({ actor: current, uploadId }) =>
    Effect.succeed({
      key: `private/${current.tenantId}/${uploadId}`,
      maxBytes: 1024 * 1024,
      allowedContentTypes: ["text/plain"],
      expiresInSeconds: 900,
      context: {},
    }),
  publish: ({ idempotencyKey, object }) =>
    Effect.sync(() => {
      publications.set(idempotencyKey, object);
      return { fileId: idempotencyKey };
    }),
}).pipe(
  Layer.provide(state),
  Layer.provide(UploadGrantDriver.proxy),
  Layer.provideMerge(MemoryStorage.layer()),
);
const routes = StorageHttp.routes({
  authentication: "bearer",
  allowedOrigins: [],
  authenticate: (request) =>
    request.headers.authorization === "Bearer demo-user"
      ? Effect.succeed(actor)
      : Effect.fail(failUpload("Unauthorized")),
  resolveDownload: ({ fileId }) => {
    const object = publications.get(fileId);
    return object
      ? Effect.succeed({ ref: object.ref, filename: "hello.txt" })
      : Effect.fail(failUpload("NotFound"));
  },
}).pipe(Layer.provide(uploads));
const server = NodeHttpServer.layer(() => createServer(), { host: "127.0.0.1", port: 0 });

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const services = yield* Layer.build(server);
      yield* Layer.build(
        HttpRouter.serve(routes, { disableLogger: true, disableListenLog: true }).pipe(
          Layer.provide(Layer.succeedContext(services)),
        ),
      );
      const baseUrl = `${HttpServer.formatAddress(Context.get(services, HttpServer.HttpServer).address)}/storage`;
      const client = yield* UploadClient.make({
        baseUrl,
        headers: () => Effect.succeed({ authorization: "Bearer demo-user" }),
      });
      const uploaded = yield* client.upload({
        file: new File(["hello"], "hello.txt", { type: "text/plain" }),
        purpose: "attachment",
        context: {},
      });
      const downloaded = yield* Effect.tryPromise(async () => {
        const response = await fetch(`${baseUrl}/files/${uploaded.result.fileId}`, {
          headers: { authorization: "Bearer demo-user" },
        });
        if (!response.ok) throw new Error(`Download failed: ${response.status}`);
        return response.text();
      });
      process.stdout.write(`${JSON.stringify({ state: uploaded.state, downloaded })}\n`);
    }),
  ),
);
