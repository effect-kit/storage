import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect, Schema } from "effect";

import { ObjectMetadata } from "@effect-kit/storage";
import type { StorageService } from "@effect-kit/storage";
import type { TransferEndpoint } from "@effect-kit/storage/transfers";

import type { ProviderSettings } from "../../../../../scripts/providers/configuration.ts";
import { ownedObjects, transferBytes } from "./fixtures.ts";
import type { SuiteReport } from "./suite.ts";

export const recoverySuite = Effect.fnUntraced(function* (
  settings: ProviderSettings,
  storage: StorageService,
  endpoint: TransferEndpoint,
  prefix: string,
  report: SuiteReport,
) {
  if ((!endpoint.multipart || !endpoint.handles) && !endpoint.offsets && !endpoint.blocks) {
    report.checks.push("durable-upload-unavailable");
    return;
  }

  const directory = yield* Effect.promise(() => mkdtemp(join(tmpdir(), "storage-live-recovery-")));
  const input = {
    settings,
    directory,
    key: `${prefix}recovered`,
    secret: randomBytes(32).toString("base64"),
  };
  const child = (mode: "interrupt" | "resume" | "cleanup") =>
    Effect.callback<number>((resume) => {
      let closed = false;
      const childProcess = spawn(
        globalThis.process.execPath,
        ["packages/storage/test/e2e/live/session-worker.ts"],
        {
          stdio: ["pipe", "pipe", "pipe"],
          env: {
            PATH: globalThis.process.env.PATH,
            NODE_ENV: "test",
            VERCEL_BLOB_RETRIES: "0",
          },
          timeout: 150000,
        },
      );
      childProcess.stdout.resume();
      childProcess.stderr.resume();
      childProcess.on("error", () => resume(Effect.succeed(1)));
      childProcess.on("close", (code) => {
        closed = true;
        resume(Effect.succeed(code ?? 1));
      });
      childProcess.stdin.on("error", () => undefined);
      childProcess.stdin.end(JSON.stringify({ ...input, mode }));
      return Effect.promise(async () => {
        if (closed || !childProcess.pid) return;
        // Join the old process before another owner opens its checkpoint namespace.
        await new Promise<void>((resolve) => {
          childProcess.once("close", () => resolve());
          childProcess.kill("SIGTERM");
        });
      });
    });
  let completed = false;
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      if (!completed && (yield* child("cleanup")) !== 0) {
        report.cleanupPending++;
        // Keep the private encrypted checkpoint when cleanup needs reconciliation.
        return;
      }
      if (report.cleanupPending === 0)
        yield* Effect.promise(() => rm(directory, { recursive: true, force: true }));
    }),
  );

  const expectExit = Effect.fnUntraced(function* (mode: "interrupt" | "resume", expected: number) {
    const code = yield* child(mode);
    if (code === expected) return;

    const failure = yield* Effect.tryPromise(() =>
      readFile(join(directory, "failure.json"), "utf8"),
    ).pipe(
      Effect.flatMap((text) =>
        Schema.decodeUnknownEffect(
          Schema.fromJsonString(
            Schema.Struct({
              operation: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9.-]{1,80}$/u)),
              reason: Schema.String.check(Schema.isPattern(/^[A-Za-z]{1,40}$/u)),
            }),
          ),
        )(text),
      ),
      Effect.catch(() =>
        Effect.succeed({ operation: "session-worker", reason: "ProviderFailure" }),
      ),
    );
    report.failure = failure;
    assert.equal(code, expected, `Fresh-process ${mode} failed`);
  });
  yield* expectExit("interrupt", 73);
  yield* expectExit("resume", 0);
  const metadata = Schema.decodeUnknownSync(ObjectMetadata)(
    JSON.parse(yield* Effect.promise(() => readFile(join(directory, "receipt.json"), "utf8"))),
  );
  assert.equal(metadata.ref.key, input.key);
  assert.equal(metadata.ref.storeId, storage.storeId);
  const objects = yield* ownedObjects(storage, prefix, report);
  objects.reserve().ref = metadata.ref;
  assert.equal((yield* storage.head({ ref: metadata.ref })).size, transferBytes);

  if (endpoint.multipart) {
    const parts = yield* Effect.promise(() => readFile(join(directory, "parts.log"), "utf8"));
    assert.equal(parts, "1\n2\n3\n", "Acknowledged part must not be uploaded again after restart");
  }
  // Session completion includes a full pinned download against the saved source digest.
  completed = true;
  report.checks.push("fresh-process-upload-recovery");
});
