import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile, mkdir, readdir, copyFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { checkArchive } from "./archive-policy.ts";
import { checkS3Consumer } from "./s3-consumer.ts";

const root = process.cwd();
const output = resolve(".artifacts/packages");
await mkdir(output, { recursive: true });
const temporary = await mkdtemp(join(tmpdir(), "effect-storage-consumer-"));
const run = (command: string, args: string[], cwd = root) =>
  execFileSync(command, args, {
    cwd,
    stdio: "pipe",
    env: { ...process.env, npm_config_offline: "true" },
  }).toString();

try {
  const packages = await readdir("packages");
  const manifests = await Promise.all(
    packages.map(
      async (directory) =>
        JSON.parse(await readFile(join("packages", directory, "package.json"), "utf8")) as {
          name: string;
          private?: boolean;
        },
    ),
  );
  const publishable = manifests
    .filter((manifest) => !manifest.private)
    .map((manifest) => manifest.name);
  assert.deepEqual(
    publishable,
    ["@effect-kit/storage"],
    "Add consumer fixtures for new publishable packages before enabling their artifact checks",
  );

  const packed = JSON.parse(
    run("pnpm", ["--dir", "packages/storage", "pack", "--pack-destination", output, "--json"]),
  ) as {
    filename: string;
  };
  const archive = resolve(output, packed.filename);
  const entries = run("tar", ["-tzf", archive])
    .trim()
    .split("\n")
    .filter((entry) => !entry.endsWith("/"));
  const manifest: unknown = JSON.parse(run("tar", ["-xOf", archive, "package/package.json"]));
  checkArchive(manifest, entries);

  const effect = JSON.parse(await readFile("node_modules/effect/package.json", "utf8")) as {
    version: string;
  };
  const typescript = JSON.parse(await readFile("node_modules/typescript/package.json", "utf8")) as {
    version: string;
  };
  await writeFile(
    join(temporary, "package.json"),
    JSON.stringify({
      name: "storage-artifact-consumer",
      private: true,
      type: "module",
      dependencies: { "@effect-kit/storage": `file:${archive}`, effect: effect.version },
      devDependencies: { typescript: typescript.version },
    }),
  );
  run(
    "pnpm",
    [
      "--ignore-workspace",
      "install",
      "--offline",
      "--ignore-scripts",
      "--config.auto-install-peers=false",
    ],
    temporary,
  );
  const fixtures = resolve("packages/storage/test/consumers");
  await Promise.all([
    copyFile(join(fixtures, "runtime.mjs"), join(temporary, "consumer.mjs")),
    copyFile(join(fixtures, "types.ts"), join(temporary, "consumer.ts")),
    copyFile(join(fixtures, "custom-adapter.ts"), join(temporary, "custom-adapter.ts")),
  ]);
  await Promise.all(
    ["NodeNext", "Bundler"].map(async (module) => {
      await writeFile(
        join(temporary, `tsconfig.${module}.json`),
        JSON.stringify({
          compilerOptions: {
            strict: true,
            noEmit: true,
            target: "ES2024",
            module: module === "Bundler" ? "ESNext" : "NodeNext",
            moduleResolution: module,
            types: [],
            lib: ["ES2024", "DOM", "DOM.Iterable"],
          },
          include: ["consumer.ts", "custom-adapter.ts"],
        }),
      );
      run(
        process.execPath,
        ["node_modules/typescript/bin/tsc", "--noEmit", "-p", `tsconfig.${module}.json`],
        temporary,
      );
    }),
  );
  run(
    process.execPath,
    [
      "node_modules/typescript/bin/tsc",
      "-p",
      "tsconfig.NodeNext.json",
      "--noEmit",
      "false",
      "--outDir",
      "build",
    ],
    temporary,
  );
  run(process.execPath, ["consumer.mjs"], temporary);
  await checkS3Consumer(temporary);
  await writeFile(
    resolve(".artifacts/artifact-check.json"),
    `${JSON.stringify(
      {
        package: "@effect-kit/storage",
        archive,
        runtime: process.version,
        checks: [
          "packed file policy",
          "production exports",
          "SDK-free isolated runtime",
          "NodeNext declarations",
          "Bundler/browser declarations",
          "memory/fault/conformance imports",
          "public custom adapter and optional capability",
          "S3-only import without signing peers",
          "explicit S3/signing/POST peers and packed public contracts",
          "SDK-free managed transfer runtime and source/sink E/R declarations",
          "isolated Node streaming digest and S3 transfer hooks",
          "SDK-free protected session runtime and source/event/batch E/R contracts",
        ],
      },
      null,
      2,
    )}\n`,
  );
  console.log(
    "Packed core/testing/transfers pass without SDK peers; S3 consumers pass with explicit peers.",
  );
} catch (error) {
  if (error !== null && typeof error === "object" && "stderr" in error) {
    process.stderr.write(String(error.stderr));
  }
  throw error;
} finally {
  await rm(temporary, { recursive: true, force: true });
}
