/* oxlint-disable no-await-in-loop -- Consumer configurations and offline installs are verified sequentially. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "vite";

export async function checkReactConsumer(
  archive: string,
  exercise?: (directory: string, bundle: string) => Promise<void>,
) {
  const directory = await mkdtemp(join(tmpdir(), "storage-react-consumer-"));
  const run = (command: string, args: string[]) =>
    execFileSync(command, args, {
      cwd: directory,
      stdio: "pipe",
      env: { ...process.env, npm_config_offline: "true" },
      timeout: 120000,
    });
  try {
    const versions: Record<string, string> = {};
    for (const name of [
      "effect",
      "react",
      "react-dom",
      "scheduler",
      "@effect/atom-react",
      "@types/react",
      "@types/react-dom",
      "typescript",
    ]) {
      const manifest = JSON.parse(
        await readFile(resolve("packages/storage/node_modules", name, "package.json"), "utf8"),
      ) as { version: string };
      versions[name] = manifest.version;
    }
    await writeFile(
      join(directory, "package.json"),
      JSON.stringify({
        private: true,
        type: "module",
        dependencies: {
          "@effect-kit/storage": `file:${archive}`,
          ...versions,
        },
      }),
    );
    run("pnpm", [
      "--ignore-workspace",
      "install",
      "--offline",
      "--ignore-scripts",
      "--config.auto-install-peers=false",
    ]);
    await copyFile(
      resolve("packages/storage/test/consumers/react-types.ts"),
      join(directory, "consumer.ts"),
    );
    await copyFile(
      resolve("packages/storage/test/consumers/react-runtime.mjs"),
      join(directory, "consumer.mjs"),
    );
    for (const resolution of ["NodeNext", "Bundler"]) {
      await writeFile(
        join(directory, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            strict: true,
            noEmit: true,
            target: "ES2024",
            module: resolution === "Bundler" ? "ESNext" : "NodeNext",
            moduleResolution: resolution,
            types: [],
            lib: ["ES2024", "DOM", "DOM.Iterable"],
          },
          include: ["consumer.ts"],
        }),
      );
      run(process.execPath, ["node_modules/typescript/bin/tsc", "-p", "tsconfig.json"]);
    }
    run(process.execPath, ["consumer.mjs"]);
    const source = resolve("packages/storage/test/react/browser.mjs");
    await copyFile(source, join(directory, "browser.mjs"));
    await build({
      root: directory,
      configFile: false,
      logLevel: "silent",
      define: { "process.env.NODE_ENV": JSON.stringify("development") },
      plugins: [
        {
          name: "reject-server-modules",
          generateBundle(_options, bundle) {
            for (const chunk of Object.values(bundle)) {
              if (chunk.type !== "chunk") continue;
              for (const name of Object.keys(chunk.modules))
                assert(
                  !/node:|@aws-sdk|@azure\/|@google-cloud|@effect\/platform-node/u.test(name),
                  name,
                );
            }
          },
        },
      ],
      build: {
        outDir: join(directory, "browser"),
        minify: false,
        lib: {
          entry: join(directory, "browser.mjs"),
          formats: ["iife"],
          name: "StorageReactProbe",
          fileName: () => "probe.js",
        },
      },
    });
    const bundle = await readFile(join(directory, "browser/probe.js"), "utf8");
    assert(!bundle.includes("__vite-browser-external"));
    await mkdir(".artifacts/react", { recursive: true });
    await writeFile(".artifacts/react/browser.js", bundle);
    if (exercise) await exercise(directory, bundle);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
