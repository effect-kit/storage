import { assert, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { HttpClient, HttpClientResponse, HttpRouter, HttpServer } from "effect/http";
import { HttpApiClient } from "effect/http-api";
import { KeyValueStore } from "effect/persistence";

import { StorageHttp, makeStorageUploadApi } from "../../src/http.js";
import { Storage } from "../../src/storage.js";
import { Uploads, failUpload, type UploadError } from "../../src/uploads.js";
import { actor, input, setup } from "./fixture.js";

const fixture = Effect.fnUntraced(function* (cookie = false, filename = "report café.txt") {
  const test = yield* setup();
  const created = yield* test.uploads.create({ actor, input, idempotencyKey: "http" });
  yield* test.uploads.content({
    actor,
    uploadId: created.id,
    body: new TextEncoder().encode("hello"),
  });
  yield* test.uploads.complete({ actor, uploadId: created.id });
  const record = yield* test.store.read(created.id);
  if (!record?.object) return yield* Effect.die(new Error("Missing object"));
  const object = record.object;
  const app = HttpRouter.toWebHandler(
    StorageHttp.routes({
      authentication: cookie ? "cookie" : "bearer",
      allowedOrigins: ["https://app.example"],
      authenticate: (request) =>
        request.headers.authorization === "Bearer fixture-user"
          ? Effect.succeed(actor)
          : Effect.fail(failUpload("Unauthorized")),
      resolveDownload: ({ fileId }) =>
        fileId === "owned-file"
          ? Effect.succeed({ ref: object.ref, filename })
          : Effect.fail(failUpload("NotFound")),
    }).pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(Storage, test.storage),
          Layer.succeed(Uploads, test.uploads),
          HttpServer.layerServices,
        ),
      ),
    ),
  );
  yield* Effect.addFinalizer(() => Effect.promise(() => app.dispose()));
  const request = (path: string, options: RequestInit = {}) =>
    Effect.promise(() =>
      app.handler(
        new Request(`https://app.example/storage${path}`, {
          ...options,
          headers: { authorization: "Bearer fixture-user", ...options.headers },
        }),
      ),
    );
  return { ...test, created, request, object, handler: app.handler };
});

it.effect("HTTP upload routes reject forged identity and cross-origin cookie mutations", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* fixture(true);
      const body = { version: 1, idempotencyKey: "new-http", input };
      assert.strictEqual(
        (yield* test.request("/uploads", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        })).status,
        403,
      );
      assert.strictEqual(
        (yield* test.request("/uploads", {
          method: "POST",
          headers: { "content-type": "application/json", origin: "https://evil.example" },
          body: JSON.stringify(body),
        })).status,
        403,
      );
      const forged = yield* test.request("/uploads", {
        method: "POST",
        headers: { "content-type": "application/json", origin: "https://app.example" },
        body: JSON.stringify({ ...body, actor: { subject: "admin" } }),
      });
      assert.strictEqual(forged.status, 400);
      const valid = yield* test.request("/uploads", {
        method: "POST",
        headers: { "content-type": "application/json", origin: "https://app.example" },
        body: JSON.stringify(body),
      });
      assert.strictEqual(valid.status, 200);
      assert.strictEqual(valid.headers.get("access-control-allow-origin"), "https://app.example");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("private HTTP downloads stream exact ranges and implement 304, 412, 416 and HEAD", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* fixture();
      const range = yield* test.request("/files/owned-file", { headers: { range: "bytes=1-3" } });
      assert.strictEqual(range.status, 206);
      assert.strictEqual(range.headers.get("content-range"), "bytes 1-3/5");
      assert.strictEqual(range.headers.get("content-length"), "3");
      assert.strictEqual(yield* Effect.promise(() => range.text()), "ell");
      assert.include(range.headers.get("content-disposition"), "caf%C3%A9");
      const unchanged = yield* test.request("/files/owned-file", {
        headers: { "if-none-match": test.object.etag ?? "" },
      });
      assert.strictEqual(unchanged.status, 304);
      assert.strictEqual(
        (yield* test.request("/files/owned-file", { headers: { "if-match": '"wrong"' } })).status,
        412,
      );
      const invalid = yield* test.request("/files/owned-file", {
        headers: { range: "bytes=9-10" },
      });
      assert.strictEqual(invalid.status, 416);
      assert.strictEqual(invalid.headers.get("content-range"), "bytes */5");
      assert.strictEqual(
        (yield* test.request("/files/owned-file", { headers: { range: "bytes=0-1,3-4" } })).status,
        416,
      );
      const head = yield* test.request("/files/owned-file", { method: "HEAD" });
      assert.strictEqual(head.headers.get("content-length"), "5");
      assert.strictEqual(yield* Effect.promise(() => head.text()), "");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "bounded HTTP JSON bodies and safe error DTOs do not expose provider or tenant internals",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const test = yield* fixture();
        const huge = yield* test.request("/uploads", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ padding: "x".repeat(300 * 1024) }),
        });
        assert.strictEqual(huge.status, 400);
        const unknown = yield* test.request(`/uploads/${"f".repeat(64)}`);
        assert.strictEqual(unknown.status, 404);
        assert.deepStrictEqual(yield* Effect.promise(() => unknown.json()), {
          _tag: "UploadError",
          code: "NotFound",
        });
      }),
    ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("generated HttpApi clients decode actual route statuses and safe errors", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* fixture();
      const wire = HttpClient.make((request, url) =>
        Effect.promise(() =>
          test.handler(
            new Request(url, {
              headers: { ...request.headers, authorization: "Bearer fixture-user" },
            }),
          ),
        ).pipe(Effect.map((response) => HttpClientResponse.fromWeb(request, response))),
      );
      const client = yield* HttpApiClient.makeWith(makeStorageUploadApi(), {
        httpClient: wire,
        baseUrl: "https://app.example",
      });
      const status = yield* client.uploads.status({ params: { id: test.created.id } });
      assert.strictEqual(status.state, "published");
      const missing = yield* Effect.result(
        client.uploads.status({ params: { id: "f".repeat(64) } }),
      );
      assert.strictEqual(missing._tag, "Failure");
      if (missing._tag === "Failure" && missing.failure._tag === "UploadError") {
        const typedCode: UploadError["code"] = missing.failure.code;
        assert.strictEqual(typedCode, "NotFound");
      }
      if (missing._tag === "Failure")
        assert.deepStrictEqual(missing.failure, { _tag: "UploadError", code: "NotFound" });
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.live("form uploads enforce one bounded file and reject filename header injection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* fixture();
      const created = yield* test.uploads.create({ actor, input, idempotencyKey: "form" });
      const form = new FormData();
      form.append("file", new Blob(["hello"], { type: "text/plain" }), "file.txt");
      const encoded = new Request("https://app.example", { method: "POST", body: form });
      const formBytes = yield* Effect.promise(() => encoded.arrayBuffer());
      assert.strictEqual(
        (yield* test.request(`/uploads/${created.id}/content`, {
          method: "POST",
          body: formBytes,
          headers: { "content-type": encoded.headers.get("content-type") ?? "" },
        })).status,
        200,
      );
      const bad = new FormData();
      bad.append("field", "x".repeat(2048));
      const badEncoded = new Request("https://app.example", { method: "POST", body: bad });
      const badBytes = yield* Effect.promise(() => badEncoded.arrayBuffer());
      const pending = yield* test.uploads.create({ actor, input, idempotencyKey: "bad-form" });
      assert.strictEqual(
        (yield* test.request(`/uploads/${pending.id}/content`, {
          method: "POST",
          body: badBytes,
          headers: { "content-type": badEncoded.headers.get("content-type") ?? "" },
        })).status,
        400,
      );
      const forged = yield* test.uploads
        .create({
          actor,
          input: { ...input, file: { ...input.file, name: "x\r\nSet-Cookie: forged" } },
          idempotencyKey: "filename",
        })
        .pipe(Effect.result);
      assert.strictEqual(forged._tag, "Failure");
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("download filenames cannot inject response headers", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* fixture(false, "report\r\nSet-Cookie: injected");
      const response = yield* test.request("/files/owned-file");
      assert.strictEqual(response.status, 400);
      assert.isNull(response.headers.get("set-cookie"));
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
