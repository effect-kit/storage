import { assert, it } from "@effect/vitest";
import { DateTime, Effect, Layer } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { KeyValueStore } from "effect/persistence";

import { StorageEventInbox } from "../../src/events.js";
import { StorageHttp } from "../../src/http.js";
import { Uploads, failUpload } from "../../src/uploads.js";
import { actor, input, setup } from "./fixture.js";

it.live(
  "callbacks authenticate before resolution, bind staging identity and deduplicate publication",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let publications = 0;
        let resolutions = 0;
        const test = yield* setup({
          publish: () =>
            Effect.sync(() => {
              publications++;
              return {};
            }),
        });
        const created = yield* test.uploads.create({ actor, input, idempotencyKey: "callback" });
        yield* test.uploads.content({
          actor,
          uploadId: created.id,
          body: new TextEncoder().encode("hello"),
        });
        const record = yield* test.store.read(created.id);
        if (!record) return assert.fail("Missing record");
        const event = {
          provider: "memory",
          kind: "object-created" as const,
          ref: { storeId: "owned", key: record.stagingKey },
          occurredAt: DateTime.makeUnsafe(0),
          id: "delivery",
          nativeType: "fixture.created",
        };
        const inbox = yield* StorageEventInbox.make({
          namespace: "callbacks",
          ownership: "exclusive",
        });
        const app = HttpRouter.toWebHandler(
          StorageHttp.callbacks({
            path: "/callbacks",
            authenticate: (request) =>
              request.headers.authorization === "verified-provider"
                ? Effect.succeed([event])
                : Effect.fail(failUpload("Unauthorized")),
            resolve: () =>
              Effect.sync(() => {
                resolutions++;
                return { actor, uploadId: created.id };
              }),
          }).pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(Uploads, test.uploads),
                Layer.succeed(StorageEventInbox, inbox),
                HttpServer.layerServices,
              ),
            ),
          ),
          { disableLogger: true },
        );
        yield* Effect.addFinalizer(() => Effect.promise(() => app.dispose()));
        const deliver = (authorization: string) =>
          Effect.promise(() =>
            app.handler(
              new Request("https://example.test/callbacks", {
                method: "POST",
                headers: { authorization },
              }),
            ),
          );
        assert.strictEqual((yield* deliver("forged")).status, 503);
        assert.strictEqual(resolutions, 0);
        assert.strictEqual((yield* deliver("verified-provider")).status, 204);
        assert.strictEqual((yield* deliver("verified-provider")).status, 204);
        assert.strictEqual(resolutions, 1);
        assert.strictEqual(publications, 1);
        const foreign = yield* Effect.result(
          test.uploads.observe({
            actor,
            uploadId: created.id,
            event: { ...event, ref: { ...event.ref, key: "other-tenant" } },
          }),
        );
        assert.strictEqual(foreign._tag, "Failure");
      }),
    ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
