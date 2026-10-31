import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { Log, LogLevel, Miniflare } from "miniflare";
import { build } from "vite";

const output = resolve(".artifacts/r2-workers");
await mkdir(output, { recursive: true });
const temporary = await mkdtemp(join(tmpdir(), "storage-r2-worker-"));
const run = (args: string[], cwd = process.cwd()) =>
  execFileSync("pnpm", args, { cwd, encoding: "utf8", stdio: "pipe" });
async function bundleWorker(directory: string, filename: string) {
  const bundled = await build({
    configFile: false,
    root: directory,
    logLevel: "silent",
    plugins: [
      {
        name: "reject-server-graph",
        resolveId(source) {
          if (
            /^(node:|@aws-sdk\/|@google-cloud\/|@azure\/|@effect\/platform-(node|bun|deno))/u.test(
              source,
            )
          )
            throw new Error(`Server import in Worker graph: ${source}`);
        },
      },
    ],
    build: {
      write: false,
      minify: false,
      target: "es2022",
      rollupOptions: {
        preserveEntrySignatures: "strict",
        input: join(directory, filename),
        output: { codeSplitting: false },
      },
    },
  });
  assert(!Array.isArray(bundled) && "output" in bundled);
  const chunk = bundled.output.find((entry) => entry.type === "chunk");
  assert(chunk?.type === "chunk" && chunk.code.length > 0);
  return chunk.code;
}

let worker: Miniflare | undefined;
let exampleWorker: Miniflare | undefined;
try {
  await writeFile(join(output, "report.json"), JSON.stringify({ status: "running" }));
  const packed = JSON.parse(
    run(["--dir", "packages/storage", "pack", "--pack-destination", output, "--json"]),
  ) as { filename: string };
  const manifest = JSON.parse(await readFile("packages/storage/package.json", "utf8")) as {
    peerDependencies: Record<string, string>;
  };
  await writeFile(
    join(temporary, "package.json"),
    JSON.stringify({
      private: true,
      type: "module",
      dependencies: {
        "@effect-kit/storage": `file:${resolve(output, packed.filename)}`,
        effect: manifest.peerDependencies.effect?.replace(/^\^/u, ""),
        "@effect/platform-browser": manifest.peerDependencies["@effect/platform-browser"]?.replace(
          /^\^/u,
          "",
        ),
      },
    }),
  );
  run(
    [
      "--ignore-workspace",
      "install",
      "--offline",
      "--ignore-scripts",
      "--config.auto-install-peers=false",
    ],
    temporary,
  );
  await copyFile(
    "packages/storage/test/fixtures/workers/r2-worker/run.mjs",
    join(temporary, "worker.mjs"),
  );
  const code = await bundleWorker(temporary, "worker.mjs");
  await writeFile(join(output, "worker.mjs"), code);
  worker = new Miniflare({
    modules: true,
    script: code,
    compatibilityDate: "2026-07-30",
    bindings: { FIXTURE_PROFILE: "owned-local" },
    outboundService: () => new Response("Outbound network disabled", { status: 502 }),
    r2Buckets: ["BUCKET"],
    r2Persist: join(temporary, "r2"),
    log: new Log(LogLevel.ERROR),
  });
  const response = await worker.dispatchFetch("http://localhost/", {
    signal: AbortSignal.timeout(60000),
  });
  const result: unknown = await response.json();
  assert.equal(response.status, 200, JSON.stringify(result));
  assert(result && typeof result === "object" && "status" in result && result.status === "passed");
  await copyFile(
    "packages/storage/test/fixtures/providers/r2-worker.mjs",
    join(temporary, "example.mjs"),
  );
  const exampleCode = await bundleWorker(temporary, "example.mjs");
  exampleWorker = new Miniflare({
    modules: true,
    script: exampleCode,
    compatibilityDate: "2026-07-30",
    r2Buckets: ["STORAGE"],
    bindings: { STORAGE_ID: "example" },
    r2Persist: join(temporary, "example-r2"),
    outboundService: () => new Response("Outbound network disabled", { status: 502 }),
    log: new Log(LogLevel.ERROR),
  });
  const exampleBucket = await exampleWorker.getR2Bucket("STORAGE");
  await exampleBucket.put("example/hello.txt", "hello");
  const exampleResponse = await exampleWorker.dispatchFetch("http://localhost/");
  assert.equal(exampleResponse.status, 200);
  assert.equal(await exampleResponse.text(), "hello");
  await writeFile(
    join(output, "report.json"),
    `${JSON.stringify(
      {
        ...result,
        host: { os: process.platform, arch: process.arch },
        environment: "miniflare@4.20260730.0/workerd@1.20260730.1",
        compatibilityDate: "2026-07-30",
        exactArchive: true,
        archiveSHA512: createHash("sha512")
          .update(await readFile(resolve(output, packed.filename)))
          .digest("hex"),
        serverFreeBundle: true,
        applicationExample: "read-only Worker executed",
      },
      null,
      2,
    )}\n`,
  );
  console.log(
    "R2 Worker: packed isolated binding conformance, streaming, multipart and managed/session integration passed",
  );
} catch (error) {
  await writeFile(join(output, "report.json"), JSON.stringify({ status: "failed" }));
  if (error && typeof error === "object" && "stderr" in error)
    process.stderr.write(String(error.stderr));
  throw error;
} finally {
  await Promise.all([worker?.dispose(), exampleWorker?.dispose()]);
  await rm(temporary, { recursive: true, force: true });
}
