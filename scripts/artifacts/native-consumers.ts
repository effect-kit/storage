/* oxlint-disable no-await-in-loop -- Each isolated consumer changes its installed manifest and type-check configuration in sequence. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

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
    ["gcs", "@google-cloud/storage"],
    ["azure", "@azure/storage-blob"],
  ] as const) {
    const temporary = await mkdtemp(join(tmpdir(), `storage-${provider}-consumer-`));
    const run = (command: string, args: string[]) =>
      execFileSync(command, args, {
        cwd: temporary,
        stdio: "pipe",
        env: { ...process.env, npm_config_offline: "true" },
      });
    try {
      const version = library.peerDependencies[sdk];
      assert(version);
      await writeFile(
        join(temporary, "package.json"),
        JSON.stringify({
          private: true,
          type: "module",
          dependencies: {
            "@effect-kit/storage": `file:${archive}`,
            effect: effect.version,
            [sdk]: version.replace(/^\^/u, ""),
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
    } finally {
      await rm(temporary, { recursive: true, force: true });
    }
  }
}
