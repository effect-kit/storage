import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { assert, it } from "@effect/vitest";
import { Effect } from "effect";

import { ListObjectVersionsCommand } from "@aws-sdk/client-s3";

import { environment, s3Fixture } from "../../fixtures/s3/acceptance.js";

const child = (
  directory: string,
  key: string,
  boundary: string,
  secret: string,
  mode = "upload",
) => {
  const result = spawnSync(
    process.execPath,
    [resolve("test/fixtures/workers/session-worker/run.mjs"), directory, key, boundary, mode],
    { timeout: 30000, encoding: "utf8", env: { ...process.env, STORAGE_SESSION_TEST_KEY: secret } },
  );
  if (result.status !== 0 && result.status !== 73)
    throw new Error(`Restart child failed at ${boundary}: ${result.stderr}`);
  return result.status;
};
const status = async (directory: string) =>
  JSON.parse(await readFile(join(directory, "result.json"), "utf8")) as {
    state: string;
    id: string;
  };

it.live.skipIf(environment.mode !== "local")(
  "separate processes recover durable uploads at remote and checkpoint boundaries",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { storage, prefix } = yield* s3Fixture();
        const root = yield* Effect.acquireRelease(
          Effect.promise(() => mkdtemp(join(tmpdir(), "storage-session-restart-"))),
          (directory) => Effect.promise(() => rm(directory, { recursive: true, force: true })),
        );
        const secret = randomBytes(32).toString("base64");
        const results: Array<{ boundary: string; state: string; singleVersionVerified: boolean }> =
          [];
        for (const boundary of [
          "created-saved",
          "created",
          "initiate-before",
          "initiate-after",
          "part-before",
          "part-after",
          "part-checkpoint",
          "complete-before",
          "complete-after",
          "verify-before",
          "completed-saved",
          "completed-before-save",
          "event-after",
          "abort-before",
          "abort-after",
        ]) {
          const directory = join(root, boundary);
          const key = `${prefix}${boundary}`;
          const mode = boundary.startsWith("abort") ? "abort" : "upload";
          assert.strictEqual(child(directory, key, boundary, secret, mode), 73, boundary);
          assert.strictEqual(child(directory, key, "none", secret, mode), 0, boundary);
          const observed = yield* Effect.promise(() => status(directory));
          const expected = boundary.startsWith("initiate")
            ? "initiating"
            : boundary === "complete-before"
              ? "completion-unknown"
              : mode === "abort"
                ? "aborted"
                : "completed";
          assert.strictEqual(observed.state, expected, boundary);
          if (expected === "completed") {
            const versions = yield* Effect.promise(() =>
              storage.client.send(
                new ListObjectVersionsCommand({ Bucket: storage.config.bucket, Prefix: key }),
              ),
            );
            assert.strictEqual(
              versions.Versions?.filter((version) => version.Key === key).length,
              1,
              boundary,
            );
            const events = JSON.parse(
              yield* Effect.promise(() => readFile(join(directory, "events.json"), "utf8")),
            ) as string[];
            assert.strictEqual(events.length, 1);
            assert.isTrue(events[0]?.endsWith(":completed"));
          }
          if (boundary === "part-checkpoint") {
            const parts = yield* Effect.promise(() =>
              readFile(join(directory, "parts.log"), "utf8"),
            );
            assert.strictEqual(parts, "1\n2\n", "Acknowledged part must not be retransmitted");
          }
          results.push({
            boundary,
            state: observed.state,
            singleVersionVerified: expected === "completed",
          });
        }
        yield* Effect.promise(() =>
          writeFile(
            "../../.artifacts/session-restart.json",
            `${JSON.stringify({ runtime: process.version, configuration: "owned-local-rustfs", results, limitations: ["Exclusive sequential process ownership; no distributed lease/CAS claim.", "Unknown initiation/completion without proof is retained, not blindly retried.", "Process termination and fsync/rename behavior, not a physical power-cut/OS matrix."] }, null, 2)}\n`,
          ),
        );
      }),
    ),
  { timeout: 180000 },
);

it.live.skipIf(environment.mode !== "local")(
  "separate processes recover synced download prefixes and publication response loss",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { storage, prefix } = yield* s3Fixture();
        const body = new Uint8Array(256 * 1024).fill(7);
        const key = `${prefix}source`;
        yield* storage.put({ key, body });
        const root = yield* Effect.acquireRelease(
          Effect.promise(() => mkdtemp(join(tmpdir(), "storage-download-restart-"))),
          (directory) => Effect.promise(() => rm(directory, { recursive: true, force: true })),
        );
        for (const boundary of ["target-write", "verified-saved", "publish-after"]) {
          const directory = join(root, boundary);
          const secret = randomBytes(32).toString("base64");
          assert.strictEqual(child(directory, key, boundary, secret, "download"), 73);
          assert.strictEqual(child(directory, key, "none", secret, "download"), 0);
          assert.strictEqual((yield* Effect.promise(() => status(directory))).state, "completed");
          assert.deepEqual(
            new Uint8Array(
              yield* Effect.promise(() => readFile(join(directory, "downloaded.bin"))),
            ),
            body,
          );
        }
      }),
    ),
  { timeout: 90000 },
);
