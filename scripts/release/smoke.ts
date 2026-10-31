/* oxlint-disable no-await-in-loop -- Registry identity, provenance and consumers are checked for each planned package. */
import { execFileSync } from "node:child_process";
import { realpath, readFile } from "node:fs/promises";

import { writeReport } from "./check.ts";
import { checkConsumer } from "./consumer.ts";
import { readCandidate, unchanged } from "./plan.ts";
import { verifyProvenance } from "./provenance.ts";
import { downloadRegistry, matchRegistry } from "./registry.ts";

export async function smoke(
  directory: string,
  provenance: boolean,
  dependencies = { download: downloadRegistry, consumer: checkConsumer },
  expected?: ReadonlyMap<string, string>,
) {
  const root = await realpath(directory);
  await writeReport(root, "registry-verification.json", { schemaVersion: 1, status: "running" });
  try {
    const candidate = await readCandidate(root, expected);
    const commit = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
    const config = JSON.parse(await readFile(".changeset/config.json", "utf8")) as {
      baseBranch: string;
    };
    const packages = [];
    for (const entry of candidate.packages) {
      const fetched = await dependencies.download(entry, provenance);
      matchRegistry(fetched.metadata, fetched.bytes, entry);
      const proof = provenance
        ? verifyProvenance(fetched.audit, entry, commit, config.baseBranch)
        : null;
      const checks = await dependencies.consumer({ ...entry, bytes: fetched.bytes });
      packages.push({
        name: entry.name,
        version: entry.version,
        integrity: entry.digest,
        checks,
        provenance: proof,
      });
    }
    await unchanged(candidate);
    const report = {
      schemaVersion: 1,
      status: "passed",
      observedAt: new Date().toISOString(),
      commit,
      provenanceRequired: provenance,
      planIntegrity: candidate.planDigest,
      packages,
    };
    await writeReport(root, "registry-verification.json", report);
    return report;
  } catch (error) {
    await writeReport(root, "registry-verification.json", { schemaVersion: 1, status: "failed" });
    throw error;
  }
}
