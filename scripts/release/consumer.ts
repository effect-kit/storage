/* oxlint-disable no-await-in-loop -- Both declaration modes validate the same isolated installed consumer sequentially. */
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { checkNativeConsumers } from "../artifacts/native-consumers.ts";
import { checkReadmeConsumer } from "../artifacts/readme.ts";
import { checkS3Consumer } from "../artifacts/s3-consumer.ts";
import { checkReactConsumer } from "../react/consumer.ts";
import type { Candidate } from "./plan.ts";

export async function checkConsumer(entry: Candidate["packages"][number]) {
  const directory = await mkdtemp(join(tmpdir(), "storage-release-consumer-"));
  const run = (command: string, args: string[]) =>
    execFileSync(command, args, {
      cwd: directory,
      stdio: "pipe",
      timeout: 120000,
      env: {
        PATH: process.env.PATH,
        HOME: process.env.HOME,
        PNPM_HOME: process.env.PNPM_HOME,
      },
    });
  try {
    const archive = join(directory, "candidate.tgz");
    await writeFile(archive, entry.bytes);
    const effect = JSON.parse(await readFile("node_modules/effect/package.json", "utf8")) as {
      version: string;
    };
    const typescript = JSON.parse(
      await readFile("node_modules/typescript/package.json", "utf8"),
    ) as { version: string };
    const peers = entry.manifest.peerDependencies as Record<string, string> | undefined;
    const peerMeta = entry.manifest.peerDependenciesMeta as
      | Record<string, { optional?: boolean }>
      | undefined;
    const required = Object.fromEntries(
      Object.entries(peers ?? {}).filter(([name]) => peerMeta?.[name]?.optional !== true),
    );
    await writeFile(
      join(directory, "package.json"),
      JSON.stringify({
        private: true,
        type: "module",
        dependencies: {
          ...required,
          [entry.name]: `file:${archive}`,
          effect: effect.version,
          typescript: typescript.version,
        },
        devDependencies: {},
      }),
    );
    run("pnpm", [
      "--ignore-workspace",
      "install",
      "--no-frozen-lockfile",
      "--prefer-offline",
      "--ignore-scripts",
      "--config.auto-install-peers=false",
    ]);
    const probe =
      entry.name === "@effect-kit/storage"
        ? `
import { Effect } from "effect";
import { Storage } from "@effect-kit/storage";
import { MemoryStorage } from "@effect-kit/storage/testing";
import { UploadAtoms } from "@effect-kit/storage/client/atoms";
if (typeof UploadAtoms.make !== "function") throw new Error("Missing atoms API");
await Effect.runPromise(Effect.gen(function* () {
  const storage = yield* Storage;
  yield* storage.put({ key: "release-probe", body: new Uint8Array([1,2,3]) });
  const bytes = yield* Storage.getBytes({ key: "release-probe", maxBytes: 3 });
  if (JSON.stringify(Array.from(bytes)) !== "[1,2,3]") throw new Error("Roundtrip mismatch");
}).pipe(Effect.scoped, Effect.provide(MemoryStorage.layer())));
`
        : `import * as Library from ${JSON.stringify(entry.name)}; if (!Object.keys(Library).length) throw new Error("Empty package API");`;
    await writeFile(join(directory, "consumer.ts"), probe);
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
    run(process.execPath, ["consumer.ts"]);
    const checks = [
      "archive-policy",
      "isolated-runtime",
      "NodeNext-declarations",
      "Bundler-declarations",
    ];
    if (entry.name === "@effect-kit/storage") {
      await checkS3Consumer(directory);
      await checkNativeConsumers(archive);
      await checkReactConsumer(archive);
      await checkReadmeConsumer(archive);
      checks.push(
        "isolated-native-provider-peers",
        "optional-React-runtime-declarations-SSR-browser",
        "actual-README-declarations-and-quick-starts",
      );
    }
    return checks;
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
