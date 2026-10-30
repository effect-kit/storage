import { randomBytes } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import { Effect, Layer, Redacted } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { KeyValueStore } from "effect/persistence";

import { GcsStorage } from "../../packages/storage/dist/gcs.js";
import { GcsUploadGrants } from "../../packages/storage/dist/gcs/uploads.js";
import { StorageHttp } from "../../packages/storage/dist/http.js";
import { aes256Gcm } from "../../packages/storage/dist/node.js";
import { S3Storage } from "../../packages/storage/dist/s3.js";
import { S3UploadGrants } from "../../packages/storage/dist/s3/uploads.js";
import { SupabaseStorage } from "../../packages/storage/dist/supabase.js";
import { SupabaseSigning } from "../../packages/storage/dist/supabase/signing.js";
import { SupabaseTusUploadGrants } from "../../packages/storage/dist/supabase/tus-uploads.js";
import { SupabaseAuthorizedUploads } from "../../packages/storage/dist/supabase/uploads.js";
import { Uploads, UploadStore, failUpload } from "../../packages/storage/dist/uploads.js";

export async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing fixture port");
  return `http://127.0.0.1:${address.port}`;
}
export async function close(server) {
  server.closeAllConnections();
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
}
export async function corsProxy(target) {
  let origin = "";
  let endpoint = "";
  let preflights = 0;
  const server = createServer((incoming, outgoing) => {
    if (incoming.headers.origin) {
      outgoing.setHeader("access-control-allow-origin", origin);
      outgoing.setHeader("access-control-allow-headers", "*");
      outgoing.setHeader(
        "access-control-allow-methods",
        "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS",
      );
      outgoing.setHeader(
        "access-control-expose-headers",
        "ETag, Range, Upload-Offset, Upload-Length, Tus-Resumable, Location",
      );
    }
    if (incoming.method === "OPTIONS" && incoming.headers["access-control-request-method"]) {
      preflights++;
      outgoing.writeHead(204);
      outgoing.end();
      return;
    }
    const upstream = httpRequest(
      target + incoming.url,
      { method: incoming.method, headers: { ...incoming.headers, host: new URL(target).host } },
      (response) => {
        const headers = { ...response.headers };
        if (headers.location) headers.location = headers.location.replace(target, endpoint);
        outgoing.writeHead(response.statusCode ?? 500, headers);
        response.pipe(outgoing);
      },
    );
    upstream.on("error", () => {
      outgoing.writeHead(502);
      outgoing.end();
    });
    incoming.on("aborted", () => upstream.destroy());
    incoming.pipe(upstream);
  });
  endpoint = await listen(server);
  return {
    endpoint,
    setOrigin: (value) => {
      origin = value;
    },
    preflights: () => preflights,
    dispose: () => close(server),
  };
}
export async function application(s3, gcs, supabase) {
  const publications = new Map();
  const failures = new Set();
  const server = createServer(async (incoming, outgoing) => {
    try {
      if (incoming.url === "/client.js") {
        outgoing.writeHead(200, { "content-type": "application/javascript" });
        outgoing.end(await readFile(".artifacts/uploads/browser/client.js"));
        return;
      }
      if (incoming.url === "/") {
        outgoing.writeHead(200, { "content-type": "text/html" });
        outgoing.end('<!doctype html><script src="/client.js"></script>');
        return;
      }
      const request = new Request(origin + incoming.url, {
        method: incoming.method,
        headers: incoming.headers,
        ...(!["GET", "HEAD"].includes(incoming.method ?? "GET")
          ? { body: incoming, duplex: "half" }
          : {}),
      });
      const response = await app.handler(request);
      outgoing.writeHead(response.status, Object.fromEntries(response.headers));
      if (!response.body) {
        outgoing.end();
        return;
      }
      await pipeline(Readable.fromWeb(response.body), outgoing);
    } catch {
      outgoing.writeHead(500);
      outgoing.end();
    }
  });
  const origin = await listen(server);
  gcs.setOrigin(origin);
  supabase.setOrigin(origin);
  const s3Layer = S3Storage.layer(s3);
  const gcsLayer = GcsStorage.layer({
    bucket: "test-bucket",
    apiEndpoint: gcs.endpoint,
    useAuthWithCustomEndpoint: false,
  });
  const supaLayer = SupabaseStorage.layer({
    url: supabase.endpoint,
    bucket: "bucket",
    storeId: "supabase-browser",
    auth: { mode: "service", token: Redacted.make("fixture-service-token") },
    resumable: true,
  });
  const userToken = Redacted.make(
    `e30.${Buffer.from(JSON.stringify({ sub: "user-a", role: "authenticated", exp: Math.floor(Date.now() / 1000) + 3600 })).toString("base64url")}.fixture-user-signature`,
  );
  const selected = [
    ["s3", s3Layer, S3UploadGrants.layer({ partBytes: 5 * 1024 ** 2 })],
    ["gcs", gcsLayer, GcsUploadGrants.layer({ chunkBytes: 256 * 1024 })],
    ["supabase", supaLayer, SupabaseAuthorizedUploads.pipe(Layer.provide(SupabaseSigning.layer()))],
    [
      "tus",
      supaLayer,
      SupabaseTusUploadGrants.layer({
        userToken: () => Effect.succeed(userToken),
        publicApiKey: Redacted.make("fixture-public-key"),
      }),
    ],
  ];
  const routes = selected.map(([name, provider, grants]) => {
    const state = UploadStore.layer({
      namespace: name,
      ownership: "exclusive",
      protection: aes256Gcm(Redacted.make(randomBytes(32))),
    }).pipe(Layer.provide(KeyValueStore.layerMemory));
    const uploads = Uploads.layer({
      quarantine: "private",
      authorize: ({ actor, uploadId, input }) =>
        Effect.succeed({
          key: `${name}/${actor.tenantId}/${uploadId}`,
          maxBytes: 32 * 1024 ** 2,
          allowedContentTypes: ["application/octet-stream"],
          expiresInSeconds: 900,
          context: { purpose: input.purpose },
        }),
      publish: ({ idempotencyKey, object, context }) => {
        publications.set(idempotencyKey, object);
        if (context.purpose === "lost-publication" && !failures.has(idempotencyKey)) {
          failures.add(idempotencyKey);
          return Effect.fail(failUpload("Unavailable"));
        }
        return Effect.succeed({ fileId: idempotencyKey });
      },
    }).pipe(Layer.provide(state), Layer.provideMerge(grants.pipe(Layer.provideMerge(provider))));
    return StorageHttp.routes({
      prefix: `/storage-${name}`,
      authentication: "cookie",
      allowedOrigins: [origin],
      authenticate: (request) =>
        request.headers.authorization === "Bearer fixture-user"
          ? Effect.succeed({ subject: "user-a", tenantId: "tenant-a" })
          : Effect.fail(failUpload("Unauthorized")),
      resolveDownload: ({ fileId }) =>
        publications.has(fileId)
          ? Effect.succeed({ ref: publications.get(fileId).ref, filename: "download.bin" })
          : Effect.fail(failUpload("NotFound")),
    }).pipe(Layer.provide(uploads));
  });
  const app = HttpRouter.toWebHandler(
    Layer.mergeAll(...routes).pipe(Layer.provide(HttpServer.layerServices)),
    { disableLogger: true },
  );
  return {
    origin,
    publications,
    dispose: async () => {
      await app.dispose();
      await close(server);
    },
  };
}
