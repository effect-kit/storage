import assert from "node:assert/strict";
import { createServer } from "node:http";

import { Effect, Redacted } from "effect";

import type { StorageService, StorageSigning } from "@effect-kit/storage";
import { chromium } from "playwright";

import { ownedObjects } from "./fixtures.ts";
import type { SuiteReport } from "./suite.ts";

export const browserSuite = Effect.fnUntraced(function* (
  storage: StorageService,
  signing: StorageSigning["Service"],
  prefix: string,
  report: SuiteReport,
) {
  const objects = yield* ownedObjects(storage, prefix, report);
  const metadata = yield* objects.put("browser", new Uint8Array([21, 22, 23]));
  const request = yield* signing.sign({ ref: metadata.ref, method: "GET", expiresInSeconds: 60 });
  const server = createServer((_request, response) =>
    response.end("<!doctype html><title>Storage acceptance</title>"),
  );
  yield* Effect.acquireRelease(
    Effect.promise(
      () =>
        new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          // A fixed origin lets resource owners allow only this test site in CORS.
          server.listen(43179, "127.0.0.1", resolve);
        }),
    ),
    () =>
      Effect.promise(
        () =>
          new Promise<void>((resolve, reject) =>
            server.close((error) => (error ? reject(error) : resolve())),
          ),
      ),
  );
  const address = server.address();
  assert(address && typeof address !== "string");
  const browser = yield* Effect.acquireRelease(
    Effect.promise(() => chromium.launch({ headless: true })),
    (instance) => Effect.promise(() => instance.close()),
  );
  const page = yield* Effect.promise(() => browser.newPage());
  yield* Effect.promise(() => page.goto(`http://127.0.0.1:${address.port}`));
  const bytes = yield* Effect.tryPromise({
    try: () =>
      page.evaluate(
        async ({ url, headers }) => {
          const response = await fetch(url, {
            headers,
            redirect: "error",
            signal: AbortSignal.timeout(15000),
          });
          if (!response.ok) throw new Error("Browser read rejected");
          return Array.from(new Uint8Array(await response.arrayBuffer()));
        },
        { url: Redacted.value(request.url), headers: Redacted.value(request.headers) },
      ),
    catch: () => new Error("Browser CORS/read qualification failed"),
  });
  assert.deepEqual(bytes, [21, 22, 23]);
  report.checks.push("browser-signed-read");
});
