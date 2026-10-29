import { runRuntime } from "./program.mjs";

const name = typeof Deno === "undefined" ? process.argv[2] : Deno.args[0];
const platform = await import(`@effect-kit/storage/${name}`);
const version =
  name === "deno" ? Deno.version.deno : name === "bun" ? Bun.version : process.version;
const report = await runRuntime(platform, `${name} ${version}`, true);
process.stdout.write(`${JSON.stringify(report)}\n`);
