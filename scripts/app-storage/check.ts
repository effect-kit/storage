/* oxlint-disable no-await-in-loop -- Each crash case exclusively owns its checkpoint directory until the child exits. */
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { Effect } from "effect";

import { chromium } from "playwright";

import { httpFixture } from "../../packages/storage/test/fixtures/supabase/http-fixture.ts";
import { httpFixture as vercelFixture } from "../../packages/storage/test/fixtures/vercel/http-fixture.ts";

const worker = resolve("packages/storage/test/fixtures/workers/native-worker/run.mjs");
const execute = promisify(execFile);
const env = { ...process.env, STORAGE_NATIVE_TEST_KEY: randomBytes(32).toString("base64") };
const results: { boundary: string; state: string }[] = [];
const temporary = await mkdtemp(join(tmpdir(), "storage-tus-restarts-"));

async function killAt(endpoint: string, directory: string, boundary: string) {
  await new Promise<void>((accept, reject) => {
    const child = spawn(
      process.execPath,
      [worker, "supabase", endpoint, directory, "crash", boundary],
      { env, stdio: ["ignore", "pipe", "pipe"] },
    );
    let stdout = "";
    let stderr = "";
    let reached = false;
    const timeout = setTimeout(() => child.kill("SIGKILL"), 30000);
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
      else reject(new Error(`Boundary ${boundary} was not reached: ${stdout}\n${stderr}`));
    });
  });
}
async function protectedFiles(directory: string, endpoint: string): Promise<void> {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await protectedFiles(path, endpoint);
    else {
      const text = await readFile(path, "utf8");
      assert(!text.includes(endpoint));
      assert(!text.includes("supabase-tus"));
      assert(!text.includes("effect-storage-operation"));
    }
  }
}
async function runExample(provider: "supabase" | "vercel", endpoint: string) {
  const directory = join(temporary, `${provider}-example`);
  await mkdir(directory);
  const file = join(directory, "source.bin");
  await writeFile(file, new Uint8Array(7 * 1024 ** 2).fill(19));
  const exampleEnv = {
    ...env,
    STORAGE_PROVIDER: provider,
    STORAGE_SUPABASE_URL: endpoint,
    STORAGE_SUPABASE_BUCKET: "bucket",
    STORAGE_SUPABASE_TOKEN: "local-user",
    STORAGE_STORE_ID: "local-example",
    STORAGE_CHECKPOINT_DIRECTORY: join(directory, "checkpoints"),
    STORAGE_CHECKPOINT_KEY: env.STORAGE_NATIVE_TEST_KEY,
    STORAGE_CONFIGURATION_ID: "local-example",
    STORAGE_VERCEL_STORE_ID: "fixture",
    STORAGE_VERCEL_ACCESS: "private",
    STORAGE_VERCEL_TOKEN: "vercel_blob_rw_fixture_secret",
    VERCEL_BLOB_API_URL: endpoint,
    VERCEL_BLOB_RETRIES: "0",
  };
  const commands =
    provider === "supabase"
      ? [
          ["put", "example/basic", file],
          ["create", "example", file, "example/durable"],
          ["resume", "example", file],
          ["status", "example"],
        ]
      : [["put", "example/basic", file]];
  for (const args of commands) {
    const output = await execute(
      process.execPath,
      [resolve("packages/storage/test/fixtures/providers/app-upload.mjs"), ...args],
      { env: exampleEnv, timeout: 30000 },
    );
    const receipt = JSON.parse(output.stdout) as {
      state?: string;
      size?: number;
      metadata?: { size?: number };
    };
    if (args[0] === "status") assert.equal(receipt.state, "completed");
    if (args[0] === "resume" || args[0] === "put")
      assert.equal(receipt.metadata?.size ?? receipt.size, 7 * 1024 ** 2);
  }
}
try {
  await writeFile(
    resolve(".artifacts/app-storage-local.json"),
    JSON.stringify({ status: "running", restarts: results }),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        yield* Effect.promise(async () => {
          for (const boundary of [
            "handle-saved",
            "write-after",
            "offset-saved",
            "final-after",
            "verifying-saved",
          ]) {
            const directory = join(temporary, boundary);
            await killAt(fixture.url, directory, boundary);
            await protectedFiles(join(directory, "checkpoints"), fixture.url);
            const output = await execute(
              process.execPath,
              [worker, "supabase", fixture.url, directory, "resume"],
              { env, timeout: 30000 },
            );
            const receipt = JSON.parse(output.stdout) as { state: string; size: number };
            assert.equal(receipt.state, "completed");
            assert.equal(receipt.size, 7 * 1024 ** 2);
            results.push({ boundary, state: receipt.state });
          }
          await runExample("supabase", fixture.url);
        });
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* vercelFixture;
        yield* Effect.promise(() => runExample("vercel", fixture.base));
      }),
    ),
  );
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const failures: string[] = [];
    page.on("pageerror", (error) => failures.push(error.message));
    for (const provider of ["supabase", "vercel"]) {
      await page.goto("about:blank");
      await page.addScriptTag({
        content: await readFile(resolve(`.artifacts/app-browser-${provider}.js`), "utf8"),
      });
      assert.equal(
        await page.evaluate(() => Reflect.get(globalThis, "storageBrowserProbe")),
        "function",
      );
    }
    assert.deepEqual(failures, []);
  } finally {
    await browser.close();
  }
  await writeFile(
    resolve(".artifacts/app-storage-local.json"),
    JSON.stringify(
      {
        status: "passed",
        runtime: process.version,
        restarts: results,
        browser: "packed Supabase and Vercel client module initialization",
        hostedQualified: false,
        examples: ["supabase-put-create-resume-status", "vercel-put"],
      },
      null,
      2,
    ),
  );
  console.log(
    `Application storage local checks passed: ${results.length} SIGKILL/restarts and two browser bundles.`,
  );
} catch (error) {
  await writeFile(
    resolve(".artifacts/app-storage-local.json"),
    JSON.stringify({ status: "failed", restarts: results }),
  );
  throw error;
} finally {
  await rm(temporary, { recursive: true, force: true });
}
