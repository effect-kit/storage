import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile, mkdir, readdir, copyFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "vite";

import { checkReactConsumer } from "../react/consumer.ts";
import { checkArchive } from "./archive-policy.ts";
import { checkNativeConsumers } from "./native-consumers.ts";
import { checkReadmeConsumer } from "./readme.ts";
import { checkS3Consumer } from "./s3-consumer.ts";

const root = process.cwd();
const output = resolve(".artifacts/packages");
await mkdir(output, { recursive: true });
const temporary = await mkdtemp(join(tmpdir(), "effect-storage-consumer-"));
const run = (command: string, args: string[], cwd = root) =>
  execFileSync(command, args, {
    cwd,
    stdio: "pipe",
    env: { ...process.env },
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
      "--prefer-offline",
      "--ignore-scripts",
      "--config.auto-install-peers=false",
    ],
    temporary,
  );
  const fixtures = resolve("packages/storage/test/fixtures/consumers");
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
  const browserEntry = join(temporary, "browser-client.js");
  await writeFile(
    browserEntry,
    'import { UploadClient, UploadClientStore } from "@effect-kit/storage/client"; import { makeStorageUploadApi } from "@effect-kit/storage/http/schema"; globalThis.storageBrowserProbe = [UploadClient.make, UploadClientStore.memory, makeStorageUploadApi];',
  );
  await build({
    root: temporary,
    configFile: false,
    logLevel: "silent",
    plugins: [
      {
        name: "verify-client-boundary",
        generateBundle(_options, bundle) {
          for (const chunk of Object.values(bundle)) {
            if (chunk.type !== "chunk") continue;
            for (const id of [
              ...Object.keys(chunk.modules),
              ...chunk.imports,
              ...chunk.dynamicImports,
            ])
              assert(
                !/node:|@aws-sdk|@google-cloud|@azure\/|@vercel\/blob/u.test(id),
                `Server module in browser client: ${id}`,
              );
          }
        },
      },
    ],
    build: {
      outDir: join(temporary, "browser"),
      minify: false,
      lib: {
        entry: browserEntry,
        formats: ["iife"],
        name: "StorageBrowserProbe",
        fileName: () => "client.js",
      },
    },
  });
  const clientBundle = await readFile(join(temporary, "browser/client.js"), "utf8");
  assert(!clientBundle.includes("__vite-browser-external"));
  // Effect contains a guarded getBuiltinModule("node:async_hooks") feature probe;
  // it does not import or bundle Node code in browsers.
  assert(
    !/from ["']node:|require\(["']node:|@aws-sdk|@google-cloud|@azure\/|@vercel\/blob|service_role/u.test(
      clientBundle,
    ),
  );
  await writeFile(resolve(".artifacts/app-browser-client.js"), clientBundle);
  await checkS3Consumer(temporary);
  await checkNativeConsumers(archive);
  await checkReactConsumer(archive);
  await checkReadmeConsumer(archive);
  await writeFile(
    resolve(".artifacts/artifact-check.json"),
    `${JSON.stringify(
      {
        package: "@effect-kit/storage",
        archive,
        runtime: process.version,
        checks: [
          "actual root and packed README snippets compile in NodeNext/Bundler; memory quick starts execute",
          "React optional-peer imports, NodeNext/Bundler declarations and SSR without client layer acquisition",
          "React browser graph and preserved client directive",
          "packed file policy",
          "production exports",
          "SDK-free isolated runtime and lifecycle/event/session-event/native-HTTP-admin imports",
          "NodeNext declarations",
          "Bundler/browser declarations",
          "memory/fault/conformance imports",
          "public custom adapter and optional capability",
          "S3-only import without signing peers",
          "explicit S3/signing/POST peers and packed public contracts",
          "isolated GCS SDK-only runtime and NodeNext/Bundler declarations",
          "isolated Azure SDK-only runtime without identity and NodeNext/Bundler declarations",
          "isolated Tigris native consumer with only its official SDK and AWS peer",
          "isolated Wasabi native consumer with only XML and AWS peers",
          "SDK-free Supabase native consumer and explicit Supabase S3 AWS peer",
          "isolated Vercel SDK-only server/client/handler NodeNext and Bundler consumers",
          "SDK-free managed transfer runtime and source/sink E/R declarations",
          "isolated core transfer construction and provider-supplied S3 driver",
          "SDK-free protected session runtime and source/event/batch E/R contracts",
          "authorized workflow/client/HttpApi imports and E/R declarations without optional peers",
          "browser client bundled from the exact packed archive without Node or cloud peers",
        ],
      },
      null,
      2,
    )}\n`,
  );
  console.log(
    "Packed core/lifecycle/events/sessions and native HTTP admin pass without SDK peers; S3, GCS, Azure, Supabase, Vercel, Tigris and Wasabi pass with their explicit peers.",
  );
} catch (error) {
  if (error !== null && typeof error === "object" && "stderr" in error) {
    process.stderr.write(String(error.stderr));
  }
  throw error;
} finally {
  await rm(temporary, { recursive: true, force: true });
}
