import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

const runtime = process.argv[2] ?? "node";
assert(runtime === "node" || runtime === "bun");
const binary = runtime === "node" ? process.execPath : "bun";
const worker = resolve("packages/storage/test/provider/filesystem-worker/run.mjs");
const execute = promisify(execFile);
const temporary = await mkdtemp(join(tmpdir(), "storage-filesystem-acceptance-"));
const invoke = async (root: string, command: string, boundary = "") => {
  const { stdout } = await execute(binary, [worker, runtime, root, command, boundary], {
    timeout: 30000,
  });
  return JSON.parse(stdout) as Record<string, unknown>;
};

async function crash(root: string, boundary: string) {
  await new Promise<void>((accept, reject) => {
    const child = spawn(binary, [worker, runtime, root, "crash", boundary], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    let reached = false;
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`Boundary timeout: ${boundary}`));
    }, 15000);
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
      else reject(new Error(`Boundary not reached: ${boundary}\n${stderr}\n${stdout}`));
    });
  });
}

try {
  const boundaries = [
    "body-written",
    "staged-synced",
    "rename-before",
    "rename-after",
    "directory-sync-before",
    "directory-sync-after",
    "link-before",
    "link-after",
  ];
  const results = await Promise.all(
    boundaries.map(async (boundary) => {
      const root = join(temporary, boundary);
      await invoke(root, "seed");
      await crash(root, boundary);
      const result = await invoke(root, "inspect");
      const committed = ["rename-after", "directory-sync-before", "directory-sync-after"].includes(
        boundary,
      );
      assert.equal(result.size, committed ? 131072 : 1);
      assert.equal(result.contentType, committed ? "new" : "old");
      assert.equal(result.first, committed ? 2 : 1);
      assert.equal(result.uniform, true);
      if (boundary === "link-before") assert.equal(result.created, undefined);
      if (boundary === "link-after")
        assert.deepEqual(result.created, { size: 131072, uniform: true });
      return { boundary, matchedGeneration: true, stagingReclaimed: result.removed };
    }),
  );
  const raceRoot = join(temporary, "race");
  await invoke(raceRoot, "seed");
  const races = await Promise.all(
    ["3", "4", "5", "6"].map((byte) => invoke(raceRoot, "race", byte)),
  );
  assert.equal(races.filter((result) => result.result === "winner").length, 1);
  assert.equal(races.filter((result) => result.result === "PreconditionFailed").length, 3);
  const conformance = await invoke(join(temporary, "conformance"), "conformance");
  const output = resolve(`.artifacts/filesystem-${runtime}.json`);
  await mkdir(resolve(".artifacts"), { recursive: true });
  await writeFile(
    output,
    `${JSON.stringify({ status: "passed", host: { os: process.platform, arch: process.arch }, runtime, conformance, results, createOnlyProcesses: 4 }, null, 2)}\n`,
  );
  console.log(
    `${runtime}: filesystem conformance, 32 MiB streaming, four-process create-only and eight crash boundaries passed`,
  );
} finally {
  await rm(temporary, { recursive: true, force: true });
}
