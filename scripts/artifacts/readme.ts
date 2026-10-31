/* oxlint-disable no-await-in-loop -- Both declaration modes validate the same isolated README consumers. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { inspectArchive } from "../release/archive.ts";

/** Compile the actual npm README, rather than maintaining a second copy of its examples. */
export async function checkReadmeConsumer(archive: string) {
  const files = inspectArchive(await readFile(archive));
  const encoded = files.get("package/README.md");
  assert(encoded, "Published README is missing");
  const packageReadme = new TextDecoder().decode(encoded);
  const rootReadme = await readFile("README.md", "utf8");
  const directory = await mkdtemp(join(tmpdir(), "storage-readme-consumer-"));
  const run = (args: string[]) =>
    execFileSync(process.execPath, args, {
      cwd: directory,
      stdio: "pipe",
      timeout: 120000,
      env: { PATH: process.env.PATH, HOME: process.env.HOME, PNPM_HOME: process.env.PNPM_HOME },
    });

  try {
    const versions: Record<string, string> = {};
    for (const name of [
      "effect",
      "typescript",
      "@types/node",
      "@effect/platform-node",
      "@aws-sdk/client-s3",
      "@aws-sdk/s3-request-presigner",
      "react",
      "scheduler",
      "@effect/atom-react",
      "@types/react",
    ]) {
      const manifest = JSON.parse(
        await readFile(
          resolve(
            name === "@types/node" ? "node_modules" : "packages/storage/node_modules",
            name,
            "package.json",
          ),
          "utf8",
        ),
      ) as { version: string };
      versions[name] = manifest.version;
    }
    await writeFile(
      join(directory, "package.json"),
      JSON.stringify({
        private: true,
        type: "module",
        dependencies: { ...versions, "@effect-kit/storage": `file:${resolve(archive)}` },
      }),
    );
    execFileSync(
      "pnpm",
      [
        "--ignore-workspace",
        "install",
        "--offline",
        "--ignore-scripts",
        "--config.auto-install-peers=false",
      ],
      {
        cwd: directory,
        stdio: "pipe",
        timeout: 120000,
        env: { PATH: process.env.PATH, HOME: process.env.HOME, PNPM_HOME: process.env.PNPM_HOME },
      },
    );

    const probes: string[] = [];
    for (const [name, markdown] of [
      ["package", packageReadme],
      ["root", rootReadme],
    ] as const) {
      const snippets = [...markdown.matchAll(/^```ts\r?\n([\s\S]*?)^```\s*$/gmu)];
      assert(snippets.length > 0, `${name} README has no checked TypeScript snippets`);
      for (const [index, match] of snippets.entries()) {
        const filename = `${name}-${index}.ts`;
        await writeFile(join(directory, filename), `${match[1]}\nexport {};\n`);
        probes.push(filename);
      }
    }
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
            types: ["node"],
            lib: ["ES2024", "DOM", "DOM.Iterable"],
          },
          include: probes,
        }),
      );
      run(["node_modules/typescript/bin/tsc", "-p", "tsconfig.json"]);
    }
    for (const name of ["package", "root"])
      assert.equal(
        run([`${name}-0.ts`])
          .toString()
          .trim(),
        "Hello from Effect",
      );
    return probes.length;
  } catch (error) {
    if (error !== null && typeof error === "object" && "stdout" in error)
      process.stderr.write(String(error.stdout));
    throw error;
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
