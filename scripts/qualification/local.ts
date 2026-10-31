/* oxlint-disable no-await-in-loop -- Local fixtures and build/artifact ownership are deliberately sequential. */
import { spawnSync } from "node:child_process";
import { parseArgs } from "node:util";

const suites = {
  s3: ["scripts/s3/local.ts"],
  native: ["scripts/native/check.ts"],
  app: ["scripts/app-storage/check.ts"],
  filesystem: ["scripts/filesystem/check.ts"],
  runtimes: ["scripts/runtime/check.ts"],
  r2: ["scripts/r2-workers/check.ts"],
  uploads: ["scripts/uploads/check.mjs"],
  composition: ["scripts/composition/check.mjs"],
  react: ["scripts/react/check.mjs"],
};
const { values } = parseArgs({
  args: process.argv.slice(2).filter((arg) => arg !== "--"),
  strict: true,
  options: {
    suite: { type: "string", default: "all" },
    runtime: { type: "string", default: "node" },
  },
});
const selected = values.suite === "all" ? Object.keys(suites) : [values.suite];
if (
  selected.some((suite) => !suite || !Object.hasOwn(suites, suite)) ||
  !["node", "bun", "deno", "browser", "all"].includes(values.runtime)
)
  throw new Error("Unknown local suite/runtime");
const run = (command: string, args: string[]) => {
  const result = spawnSync(command, args, { stdio: "inherit", timeout: 900000 });
  if (result.status !== 0) throw new Error("Local check failed");
};
// These lanes consume exact archives. Prepare them once before any fixture reads the build.
run("pnpm", ["test:artifacts"]);
for (const selectedSuite of selected) {
  const suite = selectedSuite as keyof typeof suites;
  if (
    suite === "filesystem" &&
    values.runtime !== "node" &&
    values.runtime !== "bun" &&
    values.runtime !== "all"
  )
    throw new Error("Filesystem checks support node or bun");
  if (suite === "filesystem" && values.runtime === "all") {
    run(process.execPath, [...suites[suite], "node"]);
    run(process.execPath, [...suites[suite], "bun"]);
    continue;
  }
  run(process.execPath, [
    ...suites[suite],
    ...(["filesystem", "runtimes"].includes(suite) ? [values.runtime] : []),
  ]);
}
