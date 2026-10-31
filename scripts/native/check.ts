/* oxlint-disable no-await-in-loop -- Restart cases share their fixture server and run to completion before teardown. */
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { Effect } from "effect";

import { httpFixture } from "../../packages/storage/test/fixtures/gcs/http-fixture.ts";

const execute = promisify(execFile);
const image =
  "mcr.microsoft.com/azure-storage/azurite@sha256:830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5";
const container = `storage-native-${randomUUID()}`;
const temporary = await mkdtemp(join(tmpdir(), "storage-native-"));
const report = resolve(".artifacts/native-local.json");
const worker = resolve("packages/storage/test/fixtures/workers/native-worker/run.mjs");
const env = { ...process.env, STORAGE_NATIVE_TEST_KEY: randomBytes(32).toString("base64") };
let started = false;
const results: Array<{ provider: string; boundary: string; state: string }> = [];

async function waitForAzurite(endpoint: string) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const response = await fetch(endpoint, { signal: AbortSignal.timeout(500) }).catch(
      () => undefined,
    );
    const ready = response?.headers.get("server")?.startsWith("Azurite-Blob/");
    await response?.body?.cancel();
    if (ready) return;
    await new Promise<void>((accept) => setTimeout(accept, 100));
  }
  throw new Error("Owned Azurite did not become ready");
}

async function runExample(provider: string, endpoint: string, directory: string) {
  await mkdir(directory);
  const source = join(directory, "source.bin");
  await writeFile(source, new Uint8Array(600000).fill(19));
  await execute(process.execPath, [worker, provider, endpoint, directory, "example-setup"], {
    env,
    timeout: 30000,
  });
  const exampleEnv = {
    ...env,
    STORAGE_PROVIDER: provider,
    STORAGE_CHECKPOINT_DIRECTORY: join(directory, "checkpoints"),
    STORAGE_CHECKPOINT_KEY: env.STORAGE_NATIVE_TEST_KEY,
    STORAGE_CONFIGURATION_ID: "example-v1",
    GCS_BUCKET: "test-bucket",
    GCS_ENDPOINT: endpoint,
    GCS_ANONYMOUS: "true",
    AZURE_STORAGE_CONTAINER_URL: endpoint,
    AZURE_STORAGE_ACCOUNT: "storagephase",
    AZURE_STORAGE_ACCESS_KEY: Buffer.from("disposable-local-emulator-key-32").toString("base64"),
  };
  for (const args of [
    ["put", "example/basic", source],
    ["create", "example", source, "example/durable"],
    ["resume", "example", source],
    ["status", "example"],
  ]) {
    const output = await execute(
      process.execPath,
      [resolve("packages/storage/test/fixtures/providers/native-upload.mjs"), ...args],
      { env: exampleEnv, timeout: 30000 },
    );
    const receipt = JSON.parse(output.stdout) as { state?: string; size?: number };
    if (args[0] === "status") assert.equal(receipt.state, "completed");
    if (args[0] === "resume" || args[0] === "put") assert.equal(receipt.size, 600000);
  }
}

async function crash(provider: string, endpoint: string, directory: string, boundary: string) {
  await new Promise<void>((accept, reject) => {
    const child = spawn(
      process.execPath,
      [worker, provider, endpoint, directory, "crash", boundary],
      { env, stdio: ["ignore", "pipe", "pipe"] },
    );
    let stdout = "";
    let stderr = "";
    let reached = false;
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
    }, 20000);
    child.stdout.on("data", (bytes: Buffer) => {
      stdout += bytes.toString();
      if (stdout.includes(JSON.stringify({ boundary }))) {
        reached = true;
        child.kill("SIGKILL");
      }
    });
    child.stderr.on("data", (bytes: Buffer) => {
      stderr += bytes.toString();
    });
    child.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.on("close", () => {
      clearTimeout(timeout);
      if (reached) accept();
      else reject(new Error(`Boundary not reached: ${provider}/${boundary}\n${stderr}\n${stdout}`));
    });
  });
}
async function checkProtected(directory: string, endpoint: string) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await checkProtected(path, endpoint);
    else {
      const text = await readFile(path, "utf8");
      assert(!text.includes(endpoint));
      assert(!text.includes("gcs-resumable"));
      assert(!text.includes("effect-storage-operation"));
    }
  }
}
try {
  await mkdir(resolve(".artifacts"), { recursive: true });
  await writeFile(report, JSON.stringify({ status: "running" }));
  await execute("docker", [
    "run",
    "--detach",
    "--rm",
    "--name",
    container,
    "--publish",
    "127.0.0.1::10000",
    "--env",
    "AZURITE_ACCOUNTS=storagephase:ZGlzcG9zYWJsZS1sb2NhbC1lbXVsYXRvci1rZXktMzI=",
    image,
    "azurite-blob",
    "--blobHost",
    "0.0.0.0",
    "--disableTelemetry",
    "--silent",
    "--skipApiVersionCheck",
  ]);
  started = true;
  const port = (await execute("docker", ["port", container, "10000/tcp"])).stdout.trim();
  assert(/^127\.0\.0\.1:\d+$/u.test(port));
  const azure = `http://${port}/storagephase`;
  await waitForAzurite(azure);
  await execute(
    "pnpm",
    [
      "--filter",
      "@effect-kit/storage",
      "exec",
      "vitest",
      "run",
      "--config",
      "test/vitest.native.config.ts",
    ],
    { env: { ...env, STORAGE_AZURE_LOCAL_URL: azure }, timeout: 90000 },
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const gcs = yield* httpFixture;
        yield* Effect.promise(async () => {
          for (const [provider, boundaries] of [
            [
              "gcs",
              ["handle-saved", "write-after", "offset-saved", "final-after", "verifying-saved"],
            ],
            ["azure", ["stage-after", "block-saved", "commit-after", "verifying-saved"]],
          ] as const) {
            for (const boundary of boundaries) {
              const directory = join(temporary, `${provider}-${boundary}`);
              await mkdir(directory);
              const endpoint =
                provider === "gcs"
                  ? gcs.endpoint
                  : `${azure}/session${randomUUID().replaceAll("-", "")}`;
              await crash(provider, endpoint, directory, boundary);
              await checkProtected(join(directory, "checkpoints"), endpoint);
              const resumed = await execute(
                process.execPath,
                [worker, provider, endpoint, directory, "resume"],
                { env, timeout: 30000 },
              );
              const receipt = JSON.parse(resumed.stdout) as { state: string; size: number };
              assert.equal(receipt.state, "completed");
              assert.equal(receipt.size, 600000);
              results.push({ provider, boundary, state: receipt.state });
            }
            const endpoint =
              provider === "gcs"
                ? gcs.endpoint
                : `${azure}/example${randomUUID().replaceAll("-", "")}`;
            await runExample(provider, endpoint, join(temporary, `${provider}-example`));
          }
          await execute(
            process.execPath,
            [resolve("packages/storage/test/fixtures/workers/native-worker/signing.mjs")],
            { env, timeout: 30000 },
          );
        });
      }),
    ),
  );
  await writeFile(
    report,
    JSON.stringify(
      {
        status: "passed",
        runtime: process.version,
        image,
        sdkApiVersion: "2026-10-06",
        emulatorApiVersionCheck: "explicitly-skipped",
        hostedQualified: false,
        examples: ["gcs-basic-and-durable", "azure-basic-and-durable", "native-signing"],
        restarts: results,
      },
      null,
      2,
    ),
  );
  console.log(
    `Native local acceptance passed: Azure conformance and ${results.length} process restart boundaries.`,
  );
} catch (error) {
  await writeFile(report, JSON.stringify({ status: "failed", restarts: results }));
  throw error;
} finally {
  if (started) await execute("docker", ["stop", container]);
  await rm(temporary, { recursive: true, force: true });
}
