import { execFileSync } from "node:child_process";
import { copyFile, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

export async function checkS3Consumer(temporary: string): Promise<void> {
  const manifestPath = join(temporary, "package.json");
  const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as {
    dependencies: Record<string, string>;
    devDependencies: Record<string, string>;
  };
  const versions = JSON.parse(await readFile("packages/storage/package.json", "utf8")) as {
    peerDependencies: Record<string, string>;
  };
  const run = (args: string[]) =>
    execFileSync("pnpm", args, {
      cwd: temporary,
      stdio: "pipe",
      env: { ...process.env, npm_config_offline: "true" },
    });
  const install = () =>
    run([
      "--ignore-workspace",
      "install",
      "--offline",
      "--ignore-scripts",
      "--config.auto-install-peers=false",
    ]);
  const sdk = "@aws-sdk/client-s3";
  const clientVersion = versions.peerDependencies[sdk];
  if (!clientVersion) throw new Error("S3 peer version is missing");
  manifest.dependencies[sdk] = clientVersion.replace(/^\^/u, "");
  await writeFile(manifestPath, JSON.stringify(manifest));
  install();
  await copyFile(
    resolve("packages/storage/test/fixtures/consumers/s3-only.mjs"),
    join(temporary, "s3-only.mjs"),
  );
  execFileSync(process.execPath, ["s3-only.mjs"], { cwd: temporary, stdio: "pipe" });

  for (const name of ["@aws-sdk/s3-request-presigner", "@aws-sdk/s3-presigned-post"]) {
    const version = versions.peerDependencies[name];
    if (!version) throw new Error("Signing peer version is missing");
    manifest.dependencies[name] = version.replace(/^\^/u, "");
  }
  const node = JSON.parse(await readFile("node_modules/@types/node/package.json", "utf8")) as {
    version: string;
  };
  manifest.devDependencies["@types/node"] = node.version;
  await writeFile(manifestPath, JSON.stringify(manifest));
  install();
  await Promise.all(
    ["s3-runtime.mjs", "s3-types.ts"].map((name) =>
      copyFile(resolve("packages/storage/test/fixtures/consumers", name), join(temporary, name)),
    ),
  );
  await writeFile(
    join(temporary, "tsconfig.s3.json"),
    JSON.stringify({
      compilerOptions: {
        strict: true,
        noEmit: true,
        target: "ES2024",
        module: "NodeNext",
        moduleResolution: "NodeNext",
        types: ["node"],
        lib: ["ES2024", "DOM", "DOM.Iterable"],
      },
      include: ["s3-types.ts"],
    }),
  );
  execFileSync(process.execPath, ["node_modules/typescript/bin/tsc", "-p", "tsconfig.s3.json"], {
    cwd: temporary,
    stdio: "pipe",
  });
  execFileSync(process.execPath, ["s3-runtime.mjs"], { cwd: temporary, stdio: "pipe" });
}
