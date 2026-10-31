/* oxlint-disable no-await-in-loop -- Fresh processes keep direct and adapter measurements independent. */
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { cpus, platform, arch, release } from "node:os";
import { resolve } from "node:path";

import { buildDigest } from "../providers/artifact.ts";

const root = resolve(".artifacts/qualification");
await mkdir(root, { recursive: true });
const filename = resolve(root, "hardening.json");
await writeFile(filename, JSON.stringify({ status: "running" }));
try {
  const digest = await buildDigest();
  const measurements = [];
  for (const mode of ["direct", "adapter"]) {
    const result = execFileSync(
      process.execPath,
      [
        "--expose-gc",
        "--max-old-space-size=256",
        "packages/storage/test/e2e/qualification/resources.ts",
        mode,
      ],
      { encoding: "utf8", timeout: 180000 },
    );
    measurements.push(JSON.parse(result));
  }
  execFileSync(process.execPath, ["scripts/qualification/restore.ts"], {
    stdio: "inherit",
    timeout: 120000,
  });
  if ((await buildDigest()) !== digest) throw new Error("Build changed during qualification");
  const effect = JSON.parse(await readFile("node_modules/effect/package.json", "utf8")) as {
    version: string;
  };
  await writeFile(
    filename,
    JSON.stringify(
      {
        status: "passed",
        observedAt: new Date().toISOString(),
        buildDigest: digest,
        runtime: process.version,
        effect: effect.version,
        host: { platform: platform(), release: release(), arch: arch(), cpu: cpus()[0]?.model },
        measurements,
        checks: [
          "320MiB-stream-integrity",
          "bounded-RSS",
          "descriptor-return",
          "64-cancellations",
          "128-small-files",
          "empty-staging",
          "fresh-process-transform-restore",
        ],
        scope:
          "Local Node filesystem fixture; no hosted SDK, distributed, other OS or long soak qualification",
      },
      null,
      2,
    ),
  );
  process.stdout.write(
    "Local resource and restoration checks passed. Report: .artifacts/qualification/hardening.json\n",
  );
} catch (error) {
  await writeFile(filename, JSON.stringify({ status: "failed" }));
  throw error;
}
