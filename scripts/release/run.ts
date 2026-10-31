import { releaseOptions, verifyCandidate } from "./check.ts";
import { smoke } from "./smoke.ts";

const [mode, ...args] = process.argv.slice(2);
try {
  const options = releaseOptions(args);
  const report =
    mode === "check"
      ? await verifyCandidate(options.directory)
      : mode === "smoke"
        ? await smoke(options.directory, options.provenance)
        : undefined;
  if (!report) throw new Error("Expected check or smoke");
  process.stdout.write(`${mode}: verified ${report.packages.length} exact packages.\n`);
} catch {
  process.stderr.write(
    "Release verification failed; no inputs were rebuilt, repacked or published.\n",
  );
  process.exitCode = 1;
}
