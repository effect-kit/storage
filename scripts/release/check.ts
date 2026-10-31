/* oxlint-disable no-await-in-loop -- Every planned package is verified before a successful report is written. */
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { realpath, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { parseArgs } from "node:util";

import { checkConsumer } from "./consumer.ts";
import { readCandidate, unchanged } from "./plan.ts";

export async function writeReport(root: string, name: string, report: unknown) {
  const temporary = join(root, `.${randomUUID()}.json`);
  await writeFile(temporary, JSON.stringify(report, null, 2), { mode: 0o600 });
  await rename(temporary, join(root, name));
}
export async function verifyCandidate(
  directory: string,
  consumer = checkConsumer,
  expected?: ReadonlyMap<string, string>,
) {
  const root = await realpath(directory);
  await writeReport(root, "verification.json", { schemaVersion: 1, status: "running" });
  try {
    const candidate = await readCandidate(root, expected);
    const packages = [];
    for (const entry of candidate.packages)
      packages.push({
        name: entry.name,
        version: entry.version,
        integrity: entry.digest,
        planIntegrity: entry.tarball.integrity,
        checks: await consumer(entry),
      });
    await unchanged(candidate);
    const report = {
      schemaVersion: 1,
      status: "passed",
      observedAt: new Date().toISOString(),
      commit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
      planIntegrity: candidate.planDigest,
      packages,
    };
    await writeReport(candidate.root, "verification.json", report);
    return report;
  } catch (error) {
    await writeReport(root, "verification.json", { schemaVersion: 1, status: "failed" });
    throw error;
  }
}
export const releaseOptions = (args: ReadonlyArray<string>) => {
  const { values } = parseArgs({
    args: args.filter((arg) => arg !== "--"),
    strict: true,
    options: { "pack-dir": { type: "string" }, provenance: { type: "boolean", default: false } },
  });
  if (!values["pack-dir"]) throw new Error("--pack-dir is required");
  return { directory: values["pack-dir"], provenance: values.provenance === true };
};
