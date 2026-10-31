/* oxlint-disable no-await-in-loop -- Fixture readiness and crash/resume phases are ordered. */
import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { S3Client, CreateBucketCommand, PutBucketVersioningCommand } from "@aws-sdk/client-s3";

const image =
  "rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2";
const container = `storage-composition-${randomUUID()}`;
const root = await mkdtemp(join(tmpdir(), "storage-composition-"));
const credentials = {
  accessKeyId: randomBytes(12).toString("hex"),
  secretAccessKey: randomBytes(24).toString("hex"),
};
const checks = [];
let sdk;
let started = false;
try {
  const environment = join(root, "fixture.env");
  await writeFile(
    environment,
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
      environment,
      image,
    ],
    { stdio: "pipe" },
  );
  started = true;
  const inspect = JSON.parse(execFileSync("docker", ["inspect", container], { encoding: "utf8" }));
  const port = inspect[0]?.NetworkSettings.Ports["9000/tcp"]?.[0]?.HostPort;
  assert(port);
  const endpoint = `http://127.0.0.1:${port}`;
  const bucket = `composition-${randomUUID()}`;
  const otherBucket = `composition-copy-${randomUUID()}`;
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
  assert(ready, "Owned RustFS did not become ready");
  await sdk.send(new CreateBucketCommand({ Bucket: otherBucket }));
  for (const selected of [bucket, otherBucket])
    await sdk.send(
      new PutBucketVersioningCommand({
        Bucket: selected,
        VersioningConfiguration: { Status: "Enabled" },
      }),
    );
  const config = join(root, "config.json");
  await writeFile(
    config,
    JSON.stringify({
      mode: "owned-local",
      root,
      endpoint,
      bucket,
      otherBucket,
      credentials,
      protectionKey: randomBytes(32).toString("base64"),
    }),
    { mode: 0o600 },
  );
  const child = resolve("packages/storage/test/e2e/providers/composition.acceptance.mjs");
  const run = (scenario, phase) =>
    spawnSync(process.execPath, [child, config, scenario, phase], {
      encoding: "utf8",
      timeout: 120000,
    });
  const baseline = run("baseline", "run");
  assert.equal(
    baseline.status,
    0,
    `Native baseline failed: ${baseline.stderr}\n${baseline.stdout}`,
  );
  checks.push({
    scenario: "filesystem/S3 migration, native S3 copy, transforms, mirror and content collection",
    state: "passed",
  });
  for (const scenario of [
    "migration-after-native",
    "migration-before-native",
    "mirror-after-primary",
    "mirror-delete-after-primary",
    "content-after-pointer",
    "content-before-pointer",
    "gc-after-native-delete",
  ]) {
    const killed = run(scenario, "crash");
    assert.equal(
      killed.signal,
      "SIGKILL",
      `${scenario} did not hit its boundary: ${killed.stderr}\n${killed.stdout}`,
    );
    const resumed = run(scenario, "resume");
    assert.equal(
      resumed.status,
      0,
      `${scenario} recovery failed: ${resumed.stderr}\n${resumed.stdout}`,
    );
    checks.push({ scenario, interruption: "SIGKILL", state: "passed" });
  }
  await mkdir(".artifacts/composition", { recursive: true });
  await writeFile(
    ".artifacts/composition/acceptance.json",
    JSON.stringify(
      { status: "passed", runtime: process.version, image, hostedQualified: false, checks },
      null,
      2,
    ),
  );
  process.stdout.write(
    `Composition acceptance passed: ${checks.length} cases with owned RustFS, filesystem storage and real protected file-journal process recovery.\n`,
  );
} catch (error) {
  await mkdir(".artifacts/composition", { recursive: true });
  await writeFile(
    ".artifacts/composition/acceptance.json",
    JSON.stringify(
      { status: "failed", runtime: process.version, image, hostedQualified: false, checks },
      null,
      2,
    ),
  );
  throw error;
} finally {
  sdk?.destroy();
  if (started) execFileSync("docker", ["rm", "-f", container], { stdio: "pipe" });
  await rm(root, { recursive: true, force: true });
}
