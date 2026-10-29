import { execFileSync, spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { CreateBucketCommand, PutBucketVersioningCommand, S3Client } from "@aws-sdk/client-s3";

const image =
  "rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2";
const temporary = await mkdtemp(join(tmpdir(), "storage-s3-local-"));
const container = `storage-s3-${randomUUID()}`;
const credentials = {
  accessKeyId: randomBytes(12).toString("hex"),
  secretAccessKey: randomBytes(24).toString("hex"),
};
const Bucket = `storage-${randomUUID()}`;
let started = false;
let client: S3Client | undefined;

try {
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
  const inspect = JSON.parse(
    execFileSync("docker", ["inspect", container], { encoding: "utf8" }),
  ) as Array<{ NetworkSettings: { Ports: Record<string, Array<{ HostPort: string }>> } }>;
  const port = inspect[0]?.NetworkSettings.Ports["9000/tcp"]?.[0]?.HostPort;
  if (!port) throw new Error("Owned S3 fixture has no mapped port");
  const endpoint = `http://127.0.0.1:${port}`;
  client = new S3Client({
    region: "us-east-1",
    endpoint,
    forcePathStyle: true,
    credentials,
    maxAttempts: 1,
    requestHandler: { requestTimeout: 2000, connectionTimeout: 1000 },
  });
  const waitReady = async (remaining: number): Promise<void> => {
    try {
      await client?.send(new CreateBucketCommand({ Bucket }));
    } catch {
      if (remaining === 0) throw new Error("Owned S3 fixture did not become ready");
      await new Promise<void>((resolveDelay) => setTimeout(resolveDelay, 250));
      return waitReady(remaining - 1);
    }
  };
  await waitReady(40);
  await client.send(
    new PutBucketVersioningCommand({ Bucket, VersioningConfiguration: { Status: "Enabled" } }),
  );
  const result = spawnSync(
    "pnpm",
    ["--filter", "@effect-kit/storage", "exec", "vitest", "run", "--config", "vitest.s3.config.ts"],
    {
      stdio: "inherit",
      env: {
        ...process.env,
        AWS_ACCESS_KEY_ID: credentials.accessKeyId,
        AWS_SECRET_ACCESS_KEY: credentials.secretAccessKey,
        AWS_SESSION_TOKEN: "",
        AWS_PROFILE: "",
        STORAGE_S3_ACCEPTANCE: "local",
        STORAGE_S3_TEST_BUCKET: Bucket,
        STORAGE_S3_TEST_REGION: "us-east-1",
        STORAGE_S3_TEST_PREFIX: "acceptance/",
        STORAGE_S3_TEST_ENDPOINT: endpoint,
      },
    },
  );
  await writeFile(
    resolve(".artifacts/s3-local-fixture.json"),
    `${JSON.stringify({ image, runtime: process.version, configuration: "local-rustfs-versioned", exitCode: result.status, limitations: ["Local S3 protocol evidence; does not qualify AWS IAM, regions, encryption or hosted consistency."] }, null, 2)}\n`,
  );
  if (result.status !== 0)
    throw new Error("Local S3 acceptance failed; see .artifacts/s3-acceptance.json");
} finally {
  client?.destroy();
  if (started) execFileSync("docker", ["rm", "-f", container], { stdio: "pipe" });
  await rm(temporary, { recursive: true, force: true });
}
