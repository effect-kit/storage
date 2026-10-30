import { readFile } from "node:fs/promises";

import { validateCatalogs } from "./validate.ts";

const features: unknown = JSON.parse(await readFile("catalogs/features.json", "utf8"));
const providers: unknown = JSON.parse(await readFile("catalogs/providers.json", "utf8"));
const issues = validateCatalogs(features, providers, process.cwd());

if (issues.length) {
  throw new Error(`Catalog validation failed:\n${issues.join("\n")}`);
}

console.log(
  "Catalogs cover all 47 features and 16 provider targets; references and support evidence are valid.",
);
