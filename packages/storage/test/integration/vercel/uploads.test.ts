import { createHmac } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import { KeyValueStore } from "effect/persistence";

import { storageError } from "../../../src/core/errors.js";
import { VercelClient } from "../../../src/providers/vercel/client.js";
import { VercelStorage } from "../../../src/providers/vercel/index.js";
import { VercelUploadHandler } from "../../../src/providers/vercel/uploads.js";
import { httpFixture } from "../../fixtures/vercel/http-fixture.js";

it.live(
  "authenticates callbacks, verifies current identity and suppresses duplicate delivery",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        const storage = yield* VercelStorage.make({
          storeId: "fixture",
          access: "private",
          credential: { mode: "token", token: Redacted.make("vercel_blob_rw_fixture_secret") },
        });
        const saved = yield* storage.put({ key: "avatar", body: new Uint8Array([1]) });
        let delivered = 0;
        let authorized = 0;
        const handler = yield* VercelUploadHandler.make({
          namespace: "callbacks",
          ownership: "exclusive",
          callbackUrl: `${fixture.base}/upload`,
          authorize: () =>
            Effect.sync(() => {
              authorized++;
              return { maximumSizeInBytes: 1024 };
            }),
          onObserved: (event) =>
            Effect.sync(() => {
              assert.strictEqual(event.metadata.etag, saved.etag);
              delivered++;
            }),
        }).pipe(Effect.provideService(VercelStorage, storage));
        const event = {
          type: "blob.upload-completed",
          payload: {
            blob: {
              url: `${fixture.base}/avatar`,
              downloadUrl: `${fixture.base}/avatar`,
              pathname: "avatar",
              contentType: "application/octet-stream",
              contentDisposition: "inline",
              etag: saved.etag,
            },
            tokenPayload: "actor-a",
          },
        };
        const body = JSON.stringify(event);
        const request = (signature?: string) =>
          new Request(`${fixture.base}/upload`, {
            method: "POST",
            body,
            headers: signature ? { "x-vercel-signature": signature } : {},
          });
        const denied = yield* Effect.promise(() => handler(request()));
        assert.strictEqual(denied.status, 403);
        assert.strictEqual(delivered, 0);
        const signature = createHmac("sha256", "vercel_blob_rw_fixture_secret")
          .update(body)
          .digest("hex");
        const responses = yield* Effect.promise(() =>
          Promise.all([handler(request(signature)), handler(request(signature))]),
        );
        assert.deepStrictEqual(
          responses.map((response) => response.status),
          [200, 200],
        );
        assert.strictEqual(delivered, 1);
        assert.strictEqual(authorized, 0);
        const generation = {
          type: "blob.generate-client-token",
          payload: { pathname: "avatar", multipart: false, clientPayload: null },
        };
        const issued = yield* Effect.promise(() =>
          handler(
            new Request(`${fixture.base}/upload`, {
              method: "POST",
              body: JSON.stringify(generation),
            }),
          ),
        );
        assert.strictEqual(issued.status, 200);
        assert.strictEqual(authorized, 1);
      }),
    ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.live("refuses grants on failed application authorization", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const provider = VercelStorage.layer({
        storeId: "fixture",
        access: "private",
        credential: { mode: "token", token: Redacted.make("vercel_blob_rw_fixture_secret") },
      });
      const handler = yield* VercelUploadHandler.make({
        namespace: "denied",
        ownership: "exclusive",
        callbackUrl: `${fixture.base}/upload`,
        authorize: () => Effect.fail(storageError("authorize", "app", { _tag: "AccessDenied" })),
        onObserved: () => Effect.void,
      }).pipe(Effect.provide(provider));
      const response = yield* Effect.promise(() =>
        handler(
          new Request(`${fixture.base}/upload`, {
            method: "POST",
            body: JSON.stringify({
              type: "blob.generate-client-token",
              payload: { pathname: "avatar", multipart: false, clientPayload: null },
            }),
          }),
        ),
      );
      assert.strictEqual(response.status, 403);
      assert.strictEqual(fixture.seen.length, 0);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.live("runs the SDK client upload protocol through the authorized Effect handler", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* VercelStorage.make({
        storeId: "fixture",
        access: "private",
        credential: { mode: "token", token: Redacted.make("vercel_blob_rw_fixture_secret") },
      });
      let observed = 0;
      const handler = yield* VercelUploadHandler.make({
        namespace: "client-flow",
        ownership: "exclusive",
        callbackUrl: `${fixture.base}/upload`,
        authorize: ({ key }) =>
          Effect.sync(() => {
            assert.strictEqual(key, "client.bin");
            return { maximumSizeInBytes: 2, tokenPayload: "authorized-actor" };
          }),
        onObserved: () =>
          Effect.sync(() => {
            observed++;
          }),
      }).pipe(Effect.provideService(VercelStorage, storage));
      fixture.setUploadHandler(handler);
      const receipt = yield* VercelClient.upload({
        key: "client.bin",
        file: new Blob([new Uint8Array([1, 2])]),
        access: "private",
        handleUploadUrl: `${fixture.base}/upload`,
        clientPayload: "untrusted-actor",
      });
      assert.strictEqual(receipt.pathname, "client.bin");
      assert.strictEqual(fixture.objects.get("client.bin")?.body.length, 2);
      const dataRequest = fixture.seen.find((request) => request.method === "PUT");
      assert.isTrue(
        String(dataRequest?.headers.authorization).startsWith("Bearer vercel_blob_client_fixture_"),
      );
      assert.isFalse(String(dataRequest?.headers.authorization).includes("vercel_blob_rw"));
      assert.strictEqual(observed, 0);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
