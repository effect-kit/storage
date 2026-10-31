import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { chromium } from "playwright";
import { build } from "vite";

const root = process.cwd();
const output = resolve(".artifacts/runtime");
await mkdir(output, { recursive: true });
const selected = process.argv[2] ?? "all";
const runtimes = selected === "all" ? ["node", "bun", "deno", "browser"] : [selected];
assert(runtimes.every((runtime) => ["node", "bun", "deno", "browser"].includes(runtime)));
const run = (command: string, args: string[], cwd = root) =>
  execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: "pipe",
    env: { ...process.env, npm_config_offline: "true", DENO_NO_UPDATE_CHECK: "1" },
  });
const packed = JSON.parse(
  run("pnpm", ["--dir", "packages/storage", "pack", "--pack-destination", output, "--json"]),
) as { filename: string };
const archive = resolve(output, packed.filename);
const library = JSON.parse(await readFile("packages/storage/package.json", "utf8")) as {
  peerDependencies: Record<string, string>;
  devDependencies: Record<string, string>;
};
const catalog = JSON.parse(
  run("pnpm", [
    "--filter",
    "@effect-kit/storage",
    "exec",
    "node",
    "-p",
    "JSON.stringify({redis:require('redis/package.json').version})",
  ]),
) as { redis: string };

async function browserCheck(directory: string) {
  const result = await build({
    configFile: false,
    root: directory,
    logLevel: "silent",
    plugins: [
      {
        name: "reject-server-imports",
        resolveId(source) {
          if (/^(node:|@aws-sdk\/|@google-cloud\/|@azure\/)/u.test(source))
            throw new Error(`Server dependency in browser graph: ${source}`);
        },
      },
    ],
    build: {
      write: false,
      target: "es2022",
      minify: false,
      rollupOptions: {
        input: join(directory, "browser.mjs"),
        output: { codeSplitting: false },
      },
    },
  });
  assert(!Array.isArray(result) && "output" in result);
  const bundle = result.output.find((entry) => entry.type === "chunk");
  assert(bundle && bundle.type === "chunk");
  const server = createServer((request, response) => {
    response.setHeader(
      "content-type",
      request.url === "/runtime.js" ? "text/javascript" : "text/html",
    );
    response.end(
      request.url === "/runtime.js"
        ? bundle.code
        : '<script type="module" src="/runtime.js"></script>',
    );
  });
  await new Promise<void>((accept) => server.listen(0, "127.0.0.1", accept));
  try {
    const address = server.address();
    assert(address && typeof address !== "string");
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage();
      await page.goto(`http://127.0.0.1:${address.port}`);
      const report: unknown = await page.evaluate("globalThis.runtimeResult");
      assert(report && typeof report === "object" && "checks" in report);
      return {
        ...report,
        runtime: `chromium ${browser.version()}`,
        browserBundleRejectsServerImports: true,
      };
    } finally {
      await browser.close();
    }
  } finally {
    await new Promise<void>((accept, reject) =>
      server.close((error) => (error ? reject(error) : accept())),
    );
  }
}

async function checkRuntime(runtime: string) {
  const temporary = await mkdtemp(join(tmpdir(), "effect-storage-runtime-"));
  await writeFile(join(output, `${runtime}.json`), JSON.stringify({ runtime, status: "running" }));
  try {
    const peer = `@effect/platform-${runtime}`;
    const dependencies = {
      "@effect-kit/storage": `file:${archive}`,
      effect: library.peerDependencies.effect?.replace(/^\^/u, ""),
      [peer]: library.peerDependencies[peer]?.replace(/^\^/u, ""),
      ...(runtime === "node" ? { redis: catalog.redis } : {}),
    };
    await writeFile(
      join(temporary, "package.json"),
      JSON.stringify({ private: true, type: "module", dependencies }),
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
    await Promise.all(
      ["program.mjs", "run.mjs", "browser.mjs"].map((name) =>
        copyFile(resolve("packages/storage/test/fixtures/runtime", name), join(temporary, name)),
      ),
    );
    const declarations = await readFile(
      "packages/storage/test/fixtures/runtime/types.fixture",
      "utf8",
    );
    await writeFile(
      join(temporary, "types.ts"),
      declarations.replace("@effect-kit/storage/RUNTIME", `@effect-kit/storage/${runtime}`),
    );
    await Promise.all(
      ["NodeNext", "Bundler"].map(async (module) => {
        await writeFile(
          join(temporary, `tsconfig.${module}.json`),
          JSON.stringify({
            compilerOptions: {
              strict: true,
              noEmit: true,
              target: "ES2024",
              module: module === "Bundler" ? "ESNext" : module,
              moduleResolution: module,
              types: [],
              lib: ["ES2024", "DOM", "DOM.Iterable"],
            },
            files: ["types.ts"],
          }),
        );
        run(
          process.execPath,
          [resolve("node_modules/typescript/bin/tsc"), "-p", `tsconfig.${module}.json`],
          temporary,
        );
      }),
    );
    const report =
      runtime === "browser"
        ? await browserCheck(temporary)
        : (JSON.parse(
            run(
              runtime === "node"
                ? process.execPath
                : runtime === "deno"
                  ? (process.env.STORAGE_DENO_BINARY ?? "deno")
                  : "bun",
              runtime === "deno"
                ? [
                    "run",
                    "--node-modules-dir=manual",
                    "--allow-read",
                    "--allow-write",
                    "--allow-env",
                    "--cached-only",
                    "run.mjs",
                    "deno",
                  ]
                : ["run.mjs", runtime],
              temporary,
            ),
          ) as unknown);
    await writeFile(
      join(output, `${runtime}.json`),
      `${JSON.stringify({ ...(report as object), status: "passed", declarations: ["NodeNext", "Bundler"], host: { os: process.platform, arch: process.arch } }, null, 2)}\n`,
    );
    console.log(`${runtime}: passed`);
  } catch (error) {
    await writeFile(join(output, `${runtime}.json`), JSON.stringify({ runtime, status: "failed" }));
    if (error && typeof error === "object" && "stderr" in error)
      process.stderr.write(String(error.stderr));
    throw error;
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

const results = await Promise.allSettled(runtimes.map(checkRuntime));
const failures = results.filter((result) => result.status === "rejected");
if (failures.length)
  throw new AggregateError(
    failures.map((failure) => failure.reason),
    "Runtime verification failed",
  );
