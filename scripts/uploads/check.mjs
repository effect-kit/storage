/* oxlint-disable no-await-in-loop -- Browser cases share owned services and persistent checkpoints in a deliberate sequence. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { Effect } from "effect";

import {
  CreateBucketCommand,
  PutBucketCorsCommand,
  PutBucketVersioningCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { chromium } from "playwright";
import { build } from "vite";

import { httpFixture as gcsFixture } from "../../packages/storage/test/gcs/http-fixture.ts";
import { httpFixture as supabaseFixture } from "../../packages/storage/test/supabase/http-fixture.ts";
import { application, corsProxy } from "./fixture.mjs";

const image =
  "rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2";
const container = `storage-uploads-${randomUUID()}`;
const temporary = await mkdtemp(join(tmpdir(), "storage-uploads-"));
const checks = [];
let started = false;
let sdk;
try {
  await mkdir(".artifacts/uploads/browser", { recursive: true });
  await build({
    configFile: false,
    logLevel: "silent",
    build: {
      outDir: resolve(".artifacts/uploads/browser"),
      minify: false,
      lib: {
        entry: resolve("packages/storage/test/uploads/browser.ts"),
        name: "StorageUploadAcceptance",
        formats: ["iife"],
        fileName: () => "client.js",
      },
    },
  });
  const bundle = await readFile(".artifacts/uploads/browser/client.js", "utf8");
  assert(!bundle.includes("__vite-browser-external"));
  assert(!/from ["']node:|require\(["']node:/u.test(bundle));
  assert(!bundle.includes("@aws-sdk/client-s3"));
  const credentials = {
    accessKeyId: randomBytes(12).toString("hex"),
    secretAccessKey: randomBytes(24).toString("hex"),
  };
  const envFile = join(temporary, "fixture.env");
  await writeFile(
    envFile,
    `RUSTFS_ACCESS_KEY=${credentials.accessKeyId}\nRUSTFS_SECRET_KEY=${credentials.secretAccessKey}\nRUSTFS_CONSOLE_ENABLE=false\n`,
    { mode: 0o600 },
  );
  execFileSync(
    "docker",
    [
      "run",
      "-d",
      "--rm",
      "--name",
      container,
      "--memory",
      "1g",
      "--cpus",
      "2",
      "--publish",
      "127.0.0.1::9000",
      "--tmpfs",
      "/data:rw,mode=1777",
      "--env-file",
      envFile,
      image,
    ],
    { stdio: "pipe" },
  );
  started = true;
  const inspected = JSON.parse(
    execFileSync("docker", ["inspect", container], { encoding: "utf8" }),
  );
  const port = inspected[0]?.NetworkSettings.Ports["9000/tcp"]?.[0]?.HostPort;
  assert(port);
  const endpoint = `http://127.0.0.1:${port}`;
  const bucket = `uploads-${randomUUID()}`;
  sdk = new S3Client({
    region: "us-east-1",
    endpoint,
    forcePathStyle: true,
    credentials,
    maxAttempts: 1,
    requestHandler: { requestTimeout: 3000, connectionTimeout: 1000 },
  });
  let ready = false;
  for (let attempt = 0; attempt < 40 && !ready; attempt++) {
    try {
      await sdk.send(new CreateBucketCommand({ Bucket: bucket }));
      ready = true;
    } catch {
      await new Promise((accept) => setTimeout(accept, 250));
    }
  }
  assert(ready, "Owned RustFS fixture did not become ready");
  await sdk.send(
    new PutBucketVersioningCommand({
      Bucket: bucket,
      VersioningConfiguration: { Status: "Enabled" },
    }),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const google = yield* gcsFixture;
        const supa = yield* supabaseFixture;
        yield* Effect.promise(async () => {
          const googleCors = await corsProxy(google.endpoint);
          const supaCors = await corsProxy(supa.url);
          let app;
          let browser;
          try {
            app = await application(
              { bucket, region: "us-east-1", endpoint, forcePathStyle: true, credentials },
              googleCors,
              supaCors,
            );
            await sdk.send(
              new PutBucketCorsCommand({
                Bucket: bucket,
                CORSConfiguration: {
                  CORSRules: [
                    {
                      AllowedOrigins: [app.origin],
                      AllowedMethods: ["GET", "PUT", "POST", "DELETE", "HEAD"],
                      AllowedHeaders: ["*"],
                      ExposeHeaders: ["ETag"],
                      MaxAgeSeconds: 0,
                    },
                  ],
                },
              }),
            );
            browser = await chromium.launch({ headless: true });
            const page = await browser.newPage();
            page.setDefaultTimeout(60000);
            const errors = [];
            page.on("pageerror", (error) => errors.push(error.message));
            await page.goto(app.origin);
            await page.waitForFunction(
              () => typeof globalThis.storageUploadAcceptance?.upload === "function",
            );
            for (const [provider, strategy, size, transport] of [
              ["s3", "single", 8192, "xhr"],
              ["s3", "post", 8192, "fetch"],
              ["s3", "multipart", 12 * 1024 ** 2, "fetch"],
              ["gcs", "resumable", 1024 * 1024, "fetch"],
              ["supabase", "single", 8192, "fetch"],
              ["tus", "resumable", 7 * 1024 ** 2, "fetch"],
            ]) {
              const result = await page.evaluate(
                ([name, mode, length, wire]) =>
                  globalThis.storageUploadAcceptance.upload(name, mode, length, wire),
                [provider, strategy, size, transport],
              );
              assert.equal(result.state, "published", `${provider}/${strategy}`);
              if (transport === "xhr")
                assert(result.observedSent, "XHR must report actual upload events");
              assert(result.verified);
              checks.push({ provider, strategy, state: "published", transport });
            }
            const pending = await page.evaluate(() =>
              globalThis.storageUploadAcceptance.prepare("s3", "multipart", 6 * 1024 ** 2),
            );
            assert.notEqual(pending.prematureStatus, 200);
            const checkpoint = await page.evaluate(() => JSON.stringify(localStorage));
            assert(checkpoint.includes(pending.id) && checkpoint.includes("sha256"));
            assert(
              !/X-Amz-Signature|upload-secret|fixture-service-token|url|authorization/u.test(
                checkpoint,
              ),
            );
            await page.reload();
            await page.waitForFunction(
              () => typeof globalThis.storageUploadAcceptance?.resume === "function",
            );
            const changed = await page.evaluate(
              (id) => globalThis.storageUploadAcceptance.resume("s3", id, 6 * 1024 ** 2, true),
              pending.id,
            );
            assert.equal(changed.code, "Conflict");
            const resumed = await page.evaluate(
              (id) => globalThis.storageUploadAcceptance.resume("s3", id, 6 * 1024 ** 2),
              pending.id,
            );
            assert.equal(resumed.state, "published");
            checks.push({
              provider: "s3",
              strategy: "multipart",
              restart: "page-reload",
              changedSource: "rejected",
              state: "published",
            });
            const stopped = await page.evaluate(() =>
              globalThis.storageUploadAcceptance.interrupt("s3", "multipart", 12 * 1024 ** 2),
            );
            const recovered = await page.evaluate(
              (id) => globalThis.storageUploadAcceptance.resume("s3", id, 12 * 1024 ** 2),
              stopped.id,
            );
            assert.equal(recovered.state, "published");
            checks.push({ provider: "s3", interruption: "Effect fiber", state: "published" });
            const lost = await page.evaluate(() =>
              globalThis.storageUploadAcceptance.transferred(
                "s3",
                "single",
                8192,
                "lost-publication",
              ),
            );
            const failedCommit = await page.evaluate(async (id) => {
              const response = await fetch(`/storage-s3/uploads/${id}/complete`, {
                method: "POST",
                headers: {
                  authorization: "Bearer fixture-user",
                  "content-type": "application/json",
                },
                body: "{}",
              });
              return response.status;
            }, lost.id);
            assert.equal(failedCommit, 503);
            const committedCount = app.publications.size;
            await page.reload();
            await page.waitForFunction(
              () => typeof globalThis.storageUploadAcceptance?.resume === "function",
            );
            const publicationRecovery = await page.evaluate(
              (id) => globalThis.storageUploadAcceptance.resume("s3", id, 8192),
              lost.id,
            );
            assert.equal(publicationRecovery.state, "published");
            assert.equal(app.publications.size, committedCount);
            checks.push({
              provider: "s3",
              publication: "lost acknowledgement recovered idempotently",
              state: "published",
            });
            const local = await page.evaluate(() => JSON.stringify(localStorage));
            assert(!local.includes("X-Amz-Signature"));
            assert(!local.includes("upload-secret"));
            assert(!local.includes("fixture-service-token"));
            assert(googleCors.preflights() > 0 && supaCors.preflights() > 0);
            assert.deepEqual(errors, []);
            checks.push({
              browserBundle: "no server peers",
              checkpoint: "no bearer grants",
              cors: "cross-origin preflights observed",
            });
          } finally {
            if (browser) await browser.close();
            if (app) await app.dispose();
            await googleCors.dispose();
            await supaCors.dispose();
          }
        });
      }),
    ),
  );
  await writeFile(
    ".artifacts/uploads/acceptance.json",
    JSON.stringify(
      {
        status: "passed",
        image,
        runtime: process.version,
        browser: "Chromium",
        hostedQualified: false,
        checks,
      },
      null,
      2,
    ),
  );
  console.log(
    `Authorized upload browser acceptance passed: ${checks.length} cases, real RustFS signed/multipart and native GCS/Supabase protocol fixtures with CORS.`,
  );
} catch (error) {
  await mkdir(".artifacts/uploads", { recursive: true });
  await writeFile(
    ".artifacts/uploads/acceptance.json",
    JSON.stringify({ status: "failed", hostedQualified: false, checks }, null, 2),
  );
  throw error;
} finally {
  sdk?.destroy();
  if (started) execFileSync("docker", ["rm", "-f", container], { stdio: "pipe" });
  await rm(temporary, { recursive: true, force: true });
}
