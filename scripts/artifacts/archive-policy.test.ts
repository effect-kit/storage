import assert from "node:assert/strict";
import { test } from "node:test";

import { checkArchive } from "./archive-policy.ts";

const manifest = {
  name: "@effect-kit/storage",
  version: "0.0.0",
  type: "module",
  main: "./dist/index.js",
  types: "./dist/index.d.ts",
  peerDependencies: { effect: "^4.0.6" },
  exports: {
    ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
    "./package.json": "./package.json",
  },
};
const entries = [
  "package/package.json",
  "package/README.md",
  "package/LICENSE",
  "package/dist/index.js",
  "package/dist/index.d.ts",
];

test("research, source files, secrets and traversal cannot enter the archive", () => {
  checkArchive(manifest, entries);
  for (const path of [
    "research/probe.ts",
    ".env",
    "src/index.ts",
    "dist/../../secret",
    "dist/\\secret",
  ]) {
    assert.throws(() => checkArchive(manifest, [...entries, `package/${path}`]));
  }
});

test("missing runtime/declarations/license and development exports fail", () => {
  for (const required of ["LICENSE", "dist/index.js", "dist/index.d.ts"]) {
    assert.throws(() =>
      checkArchive(
        manifest,
        entries.filter((entry) => entry !== `package/${required}`),
      ),
    );
  }
  assert.throws(() => checkArchive({ ...manifest, "workspace-source": "./src/index.ts" }, entries));
  assert.throws(() => checkArchive({ ...manifest, exports: { ".": "./src/index.ts" } }, entries));
});
