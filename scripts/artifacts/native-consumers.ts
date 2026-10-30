/* oxlint-disable no-await-in-loop -- Each isolated consumer changes its installed manifest and type-check configuration in sequence. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { build } from "vite";

/** Each native provider is checked in a fresh install containing only its chosen peers. */
export async function checkNativeConsumers(archive: string): Promise<void> {
  const library = JSON.parse(await readFile("packages/storage/package.json", "utf8")) as {
    peerDependencies: Record<string, string>;
  };
  const node = JSON.parse(await readFile("node_modules/@types/node/package.json", "utf8")) as {
    version: string;
  };
  const typescript = JSON.parse(await readFile("node_modules/typescript/package.json", "utf8")) as {
    version: string;
  };
  const effect = JSON.parse(await readFile("node_modules/effect/package.json", "utf8")) as {
    version: string;
  };
  for (const [provider, sdk] of [
    ["age", "age-encryption"],
    ["gcs", "@google-cloud/storage"],
    ["azure", "@azure/storage-blob"],
    ["supabase", undefined],
    ["vercel", "@vercel/blob"],
    ["tigris", "@tigrisdata/storage"],
    ["wasabi", "fast-xml-parser"],
  ] as const) {
    const temporary = await mkdtemp(join(tmpdir(), `storage-${provider}-consumer-`));
    const run = (command: string, args: string[]) =>
      execFileSync(command, args, {
        cwd: temporary,
        stdio: "pipe",
        env: { ...process.env, npm_config_offline: "true" },
      });
    try {
      const version = sdk ? library.peerDependencies[sdk] : undefined;
      if (sdk) assert(version);
      await writeFile(
        join(temporary, "package.json"),
        JSON.stringify({
          private: true,
          type: "module",
          dependencies: {
            "@effect-kit/storage": `file:${archive}`,
            effect: effect.version,
            ...(sdk && version ? { [sdk]: version.replace(/^\^/u, "") } : {}),
            ...(["tigris", "wasabi"].includes(provider)
              ? {
                  "@aws-sdk/client-s3": library.peerDependencies["@aws-sdk/client-s3"]?.replace(
                    /^\^/u,
                    "",
                  ),
                }
              : {}),
          },
          devDependencies: { typescript: typescript.version, "@types/node": node.version },
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
        resolve(`packages/storage/test/consumers/${provider}-types.ts`),
        join(temporary, "consumer.ts"),
      );
      await copyFile(
        resolve(`packages/storage/test/consumers/${provider}-runtime.mjs`),
        join(temporary, "consumer.mjs"),
      );
      for (const resolution of ["NodeNext", "Bundler"] as const) {
        await writeFile(
          join(temporary, "tsconfig.json"),
          JSON.stringify({
            compilerOptions: {
              strict: true,
              noEmit: true,
              target: "ES2024",
              module: resolution === "Bundler" ? "ESNext" : "NodeNext",
              moduleResolution: resolution,
              types: ["node"],
              lib: ["ES2024", "DOM", "DOM.Iterable"],
            },
            include: ["consumer.ts"],
          }),
        );
        run(process.execPath, ["node_modules/typescript/bin/tsc", "-p", "tsconfig.json"]);
      }
      run(process.execPath, ["consumer.mjs"]);
      if (provider === "supabase" || provider === "vercel") {
        const source =
          provider === "supabase"
            ? 'import { SupabaseStorage } from "@effect-kit/storage/supabase"; globalThis.storageBrowserProbe = typeof SupabaseStorage.layer;'
            : 'import { VercelClient } from "@effect-kit/storage/vercel/client"; globalThis.storageBrowserProbe = typeof VercelClient.upload;';
        const entry = join(temporary, "browser-probe.js");
        const outDir = join(temporary, "browser");
        await writeFile(entry, source);
        await build({
          root: temporary,
          configFile: false,
          logLevel: "silent",
          build: {
            outDir,
            emptyOutDir: true,
            minify: false,
            lib: {
              entry,
              formats: ["iife"],
              name: "StorageBrowserProbe",
              fileName: () => "probe.js",
            },
          },
        });
        const files = await readdir(outDir);
        assert(files.includes("probe.js"));
        const bundled = await readFile(join(outDir, "probe.js"), "utf8");
        assert(!bundled.includes("__vite-browser-external"));
        assert(!/from ["']node:|require\(["']node:/u.test(bundled));
        await writeFile(resolve(`.artifacts/app-browser-${provider}.js`), bundled);
      }
    } finally {
      await rm(temporary, { recursive: true, force: true });
    }
  }
}
