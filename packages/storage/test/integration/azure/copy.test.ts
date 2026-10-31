import { createServer } from "node:http";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";

import { AzureStorage } from "../../../src/providers/azure/index.js";

it.effect(
  "Azure SDK async copy keeps pending IDs, checks status, rejects superseded IDs and cancels",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let status = "pending";
        let requests = 0;
        const fixture = yield* Effect.acquireRelease(
          Effect.promise(async () => {
            const server = createServer((request, response) => {
              requests++;
              request.resume();
              const url = new URL(request.url ?? "/", "http://localhost");
              if (url.searchParams.get("comp") === "copy") {
                status = "aborted";
                response.writeHead(204);
                response.end();
                return;
              }
              response.writeHead(request.method === "PUT" ? 202 : 200, {
                "x-ms-copy-id": "copy-1",
                "x-ms-copy-status": status,
                "x-ms-blob-type": "BlockBlob",
                "content-length": "0",
                etag: '"copy-etag"',
                "last-modified": "Fri, 30 Oct 2026 00:00:00 GMT",
              });
              response.end();
            });
            await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
            return server;
          }),
          (server) =>
            Effect.promise(
              () =>
                new Promise<void>((resolve, reject) => {
                  server.closeAllConnections();
                  server.close((error) => (error ? reject(error) : resolve()));
                }),
            ),
        );
        const address = fixture.address();
        if (!address || typeof address === "string")
          return assert.fail("Fixture address unavailable");
        const storage = yield* AzureStorage.make({
          containerUrl: `http://127.0.0.1:${address.port}/account/container`,
        });
        const begun = yield* storage.beginCopy({
          sourceUrl: Redacted.make("https://source.example/blob?sig=private"),
          destinationKey: "copy",
        });
        assert.strictEqual(begun.copyId, "copy-1");
        assert.strictEqual((yield* storage.pollCopy(begun))._tag, "Pending");
        status = "success";
        const complete = yield* storage.pollCopy(begun);
        assert.strictEqual(complete._tag, "Completed");
        const superseded = yield* storage
          .pollCopy({ ...begun, copyId: "copy-old" })
          .pipe(Effect.result);
        assert.strictEqual(
          superseded._tag === "Failure" && superseded.failure.reason._tag,
          "PreconditionFailed",
        );
        status = "pending";
        yield* storage.cancelCopy(begun);
        assert.strictEqual((yield* storage.pollCopy(begun))._tag, "Failed");
        assert.strictEqual(requests, 6);
      }),
    ),
);
