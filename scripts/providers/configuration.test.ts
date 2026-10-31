import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { configure, options, providerIds } from "./configuration.ts";

const env = {
  STORAGE_TEST_S3_CONFIG: '{"bucket":"fixture","region":"us-east-1"}',
  STORAGE_TEST_S3_PREFIX: "effect-storage-acceptance/unit/",
  STORAGE_TEST_S3_ACCESS_KEY_ID: "unit-only",
  STORAGE_TEST_S3_SECRET_ACCESS_KEY: "canary-do-not-log",
};
test("provider selection requires exactly one explicit credentials source", () => {
  assert.throws(() => options([]));
  assert.throws(() => options(["--env", "--credentials-file", "private.env"]));
  assert.throws(() => options(["--env", "--suite", "delete-everything"]));
  assert.equal(options(["--env", "--provider", "s3"]).provider, "s3");
  assert.throws(() => configure(env));
  assert.throws(() => configure(env, "s3,s3"));
  assert.throws(() => configure(env, "../s3"));
  assert.throws(() => configure(env, "all"));
});
test("owned prefixes, HTTPS endpoints and separated credentials are enforced", () => {
  const settings = configure(env, "s3")[0];
  assert.equal(settings?.provider, "s3");
  for (const prefix of [
    "",
    "/",
    "production/",
    "effect-storage-acceptance/../",
    "effect-storage-acceptance/a/b/",
  ])
    assert.throws(() => configure({ ...env, STORAGE_TEST_S3_PREFIX: prefix }, "s3"));
  for (const endpoint of [
    "http://localhost",
    "https://user:secret@fixture.invalid",
    "https://fixture.invalid/?token=secret",
  ])
    assert.throws(() =>
      configure(
        {
          ...env,
          STORAGE_TEST_S3_CONFIG: JSON.stringify({
            bucket: "fixture",
            region: "us-east-1",
            endpoint,
          }),
        },
        "s3",
      ),
    );
  assert.throws(() =>
    configure(
      {
        ...env,
        STORAGE_TEST_S3_CONFIG: '{"bucket":"fixture","region":"us-east-1","credentials":{}}',
      },
      "s3",
    ),
  );
});

test("only local MinIO endpoints may use HTTP", () => {
  const minioEnvironment = {
    STORAGE_TEST_MINIO_PREFIX: env.STORAGE_TEST_S3_PREFIX,
    STORAGE_TEST_MINIO_ACCESS_KEY_ID: env.STORAGE_TEST_S3_ACCESS_KEY_ID,
    STORAGE_TEST_MINIO_SECRET_ACCESS_KEY: env.STORAGE_TEST_S3_SECRET_ACCESS_KEY,
  };
  const minio = (endpoint: string) =>
    configure(
      {
        ...minioEnvironment,
        STORAGE_TEST_MINIO_CONFIG: JSON.stringify({
          bucket: "fixture",
          region: "us-east-1",
          endpoint,
          edition: "aistor",
          serverVersion: "test-fixture",
          versioning: false,
        }),
      },
      "minio",
    );

  for (const endpoint of ["http://127.0.0.1:9000", "http://localhost:9000", "http://[::1]:9000"]) {
    assert.equal(minio(endpoint)[0]?.config.endpoint, endpoint);
  }

  for (const endpoint of [
    "http://minio.example.test",
    "http://10.0.0.1:9000",
    "http://localhost.example.test:9000",
    "http://127.0.0.1.example.test:9000",
    "http://localhost@minio.example.test",
    "http://user:secret@localhost:9000",
    "http://localhost:9000/?token=secret",
    "http://localhost:9000/#secret",
    "ftp://localhost:9000",
  ]) {
    assert.throws(() => minio(endpoint));
  }
});

test("example covers each provider and private dotenv stays out of diagnostics", async () => {
  const example = await readFile(".env.example", "utf8");
  for (const provider of providerIds)
    assert(example.includes(`STORAGE_TEST_${provider.toUpperCase().replaceAll("-", "_")}_CONFIG=`));
  const directory = await mkdtemp(join(tmpdir(), "storage-config-"));
  try {
    const file = join(directory, "provider.env");
    await writeFile(
      file,
      "STORAGE_TEST_PROVIDERS=s3\nSTORAGE_TEST_S3_CONFIG='invalid-canary-do-not-log'\nSTORAGE_TEST_S3_PREFIX=effect-storage-acceptance/unit/\n",
    );
    const result = spawnSync(
      process.execPath,
      ["scripts/providers/run.ts", "--credentials-file", file, "--validate-only"],
      { encoding: "utf8" },
    );
    assert.notEqual(result.status, 0);
    assert(!`${result.stdout}${result.stderr}`.includes("canary-do-not-log"));
    await writeFile(
      file,
      Object.entries(env)
        .map(([key, value]) => `${key}='${value}'`)
        .join("\n"),
    );
    const valid = spawnSync(
      process.execPath,
      [
        "scripts/providers/run.ts",
        "--credentials-file",
        file,
        "--provider",
        "s3",
        "--validate-only",
      ],
      { encoding: "utf8" },
    );
    assert.equal(valid.status, 0, valid.stderr);
    assert(!valid.stdout.includes("canary-do-not-log"));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
