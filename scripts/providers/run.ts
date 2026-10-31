/* oxlint-disable no-await-in-loop -- Each configured namespace and its cleanup are owned by one sequential child. */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

import { buildDigest } from "./artifact.ts";
import { loadSettings, options } from "./configuration.ts";
import { assessReport } from "./report.ts";

const runId = randomUUID();
const directory = resolve(".artifacts/providers", runId);
await mkdir(directory, { recursive: true });
const reportPath = resolve(directory, "report.json");
const report: {
  status: string;
  runId: string;
  runtime: string;
  cases: unknown[];
  reason?: string;
  buildDigest?: string;
  observedAt: string;
} = {
  status: "failed",
  runId,
  runtime: process.version,
  observedAt: new Date().toISOString(),
  cases: [],
};
try {
  const args = options(process.argv.slice(2));
  const settings = await loadSettings(args);
  if (args.suite !== "portable" && settings.some((profile) => profile.provider === "r2-workers"))
    throw new Error("WorkerSigningUnsupported");
  if (args["validate-only"]) {
    process.stdout.write(
      `Configuration valid for ${settings.map((profile) => profile.provider).join(", ")}. No network access.\n`,
    );
    report.status = "configuration-only";
  } else {
    const built = spawnSync("pnpm", ["build"], {
      stdio: "inherit",
      timeout: 180000,
      env: { PATH: process.env.PATH, HOME: process.env.HOME, PNPM_HOME: process.env.PNPM_HOME },
    });
    if (built.status !== 0) throw new Error("BuildFailed");
    const artifactDigest = await buildDigest();
    report.buildDigest = artifactDigest;
    for (const profile of settings) {
      const childReport = resolve(directory, `${profile.provider}.json`);
      const payload = {
        settings: { ...profile, prefix: `${profile.prefix}${runId}/` },
        suite: args.suite,
        artifactDigest,
        challenge: randomUUID(),
        reportPath: childReport,
      };
      const code = await new Promise<number>((accept) => {
        const child = spawn(process.execPath, ["packages/storage/test/e2e/live/run.ts"], {
          stdio: ["pipe", "pipe", "pipe"],
          env: { PATH: process.env.PATH, VERCEL_BLOB_RETRIES: "0", NODE_ENV: "test" },
          timeout: args.suite === "release" ? 600000 : 210000,
        });
        child.stdout.resume();
        child.stderr.resume();
        child.on("error", () => accept(1));
        child.on("exit", (status) => accept(status ?? 1));
        child.stdin.on("error", () => undefined);
        child.stdin.end(JSON.stringify(payload));
      });
      let result: unknown;
      try {
        result = JSON.parse(await readFile(childReport, "utf8"));
      } catch {
        result = undefined;
      }
      const { passed, evidence } = assessReport(result, code, {
        provider: profile.provider,
        suite: args.suite ?? "portable",
        prefix: payload.settings.prefix,
      });
      report.cases.push(evidence);
      process.stdout.write(`${profile.provider}: ${passed ? "passed" : "failed"}\n`);
      if (!passed) report.reason = "ProviderOrCleanupFailed";
    }
    if ((await buildDigest()) !== artifactDigest) throw new Error("BuildChangedDuringRun");
    if (!report.reason) report.status = "passed";
  }
} catch (error) {
  // Configuration errors contain field names only. Never print decoded config or SDK diagnostics.
  report.reason =
    error instanceof Error && /^Missing STORAGE_TEST_[A-Z_]+$/u.test(error.message)
      ? error.message
      : "ConfigurationOrRunnerFailed";
  process.stderr.write(
    `${report.reason}. Check selected providers and explicit credentials source.\n`,
  );
} finally {
  await writeFile(reportPath, JSON.stringify(report, null, 2), { mode: 0o600 });
  process.stdout.write(`Provider report: .artifacts/providers/${runId}/report.json\n`);
  if (!["passed", "configuration-only"].includes(report.status)) process.exitCode = 1;
}
