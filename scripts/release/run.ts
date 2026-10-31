import { releaseOptions, verifyCandidate } from "./check.ts";
import { releaseFailure } from "./failure.ts";
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
} catch (error) {
  const failure = releaseFailure(error, mode ?? "arguments");
  process.stderr.write(
    `Release verification failed (${failure.code}); no inputs were rebuilt, repacked or published.\n`,
  );
  process.exitCode = 1;
}
