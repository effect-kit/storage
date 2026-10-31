import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { Log, LogLevel, Miniflare } from "miniflare";
import { build } from "vite";

import { buildDigest } from "./artifact.ts";

await mkdir(".artifacts/providers", { recursive: true });
const digest = await buildDigest();
const bundled = await build({
  configFile: false,
  logLevel: "silent",
  define: { storageLiveBuildId: JSON.stringify(digest) },
  build: {
    write: false,
    minify: false,
    target: "es2022",
    rollupOptions: {
      input: resolve("packages/storage/test/provider/live/worker.ts"),
      preserveEntrySignatures: "strict",
      output: { codeSplitting: false },
    },
  },
});
assert(!Array.isArray(bundled) && "output" in bundled);
const code = bundled.output.find((entry) => entry.type === "chunk");
assert(code?.type === "chunk");
await writeFile(".artifacts/providers/worker.js", code.code);
await writeFile(".artifacts/providers/worker-build.json", JSON.stringify({ digest }));
const token = randomUUID();
const worker = new Miniflare({
  modules: true,
  script: code.code,
  compatibilityDate: "2026-07-30",
  r2Buckets: ["BUCKET"],
  bindings: { STORAGE_TEST_TOKEN: token, STORAGE_TEST_PREFIX: "effect-storage-acceptance/local/" },
  log: new Log(LogLevel.ERROR),
  outboundService: () => new Response(null, { status: 502 }),
});
try {
  assert.equal((await worker.dispatchFetch("https://fixture/", { method: "POST" })).status, 403);
  const call = (prefix: string) =>
    worker.dispatchFetch("https://fixture/", {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: JSON.stringify({ prefix, suite: "portable", challenge: "local" }),
    });
  assert.equal((await call(`effect-storage-acceptance/other/${randomUUID()}/`)).status, 400);
  const response = await call(`effect-storage-acceptance/local/${randomUUID()}/`);
  const report = (await response.json()) as { status: string; buildId: string };
  assert.equal(response.status, 200, JSON.stringify(report));
  assert.equal(report.status, "passed");
  assert.equal(report.buildId, digest);
  assert.equal((await (await worker.getR2Bucket("BUCKET")).list()).objects.length, 0);
  process.stdout.write(
    "Native Worker fixture built and locally verified; no deployment performed.\n",
  );
} finally {
  await worker.dispose();
}
