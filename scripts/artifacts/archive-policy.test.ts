import assert from "node:assert/strict";
import { test } from "node:test";

import { checkArchive } from "./archive-policy.ts";

const manifest = {
  name: "@effect-kit/storage",
  version: "0.0.0",
  type: "module",
  main: "./dist/index.js",
  types: "./dist/index.d.ts",
  peerDependencies: {
    effect: "^4.0.6",
    "@aws-sdk/client-s3": "^3.1159.0",
    "@aws-sdk/s3-request-presigner": "^3.1159.0",
    "@aws-sdk/s3-presigned-post": "^3.1159.0",
  },
  peerDependenciesMeta: {
    "@aws-sdk/client-s3": { optional: true },
    "@aws-sdk/s3-request-presigner": { optional: true },
    "@aws-sdk/s3-presigned-post": { optional: true },
  },
  exports: {
    ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
    "./testing": { types: "./dist/testing.d.ts", import: "./dist/testing.js" },
    "./package.json": "./package.json",
    "./s3": { types: "./dist/s3.d.ts", import: "./dist/s3.js" },
    "./s3/signing": { types: "./dist/s3/signing.d.ts", import: "./dist/s3/signing.js" },
    "./transfers": { types: "./dist/transfers.d.ts", import: "./dist/transfers.js" },
    "./transfers/node": { types: "./dist/transfers/node.d.ts", import: "./dist/transfers/node.js" },
    "./s3/transfers": { types: "./dist/s3/transfers.d.ts", import: "./dist/s3/transfers.js" },
    "./s3/post": { types: "./dist/s3/post.d.ts", import: "./dist/s3/post.js" },
  },
};
const entries = [
  "package/package.json",
  "package/README.md",
  "package/LICENSE",
  "package/dist/index.js",
  "package/dist/index.d.ts",
  "package/dist/testing.js",
  "package/dist/testing.d.ts",
  "package/dist/s3.js",
  "package/dist/s3.d.ts",
  "package/dist/s3/signing.js",
  "package/dist/s3/signing.d.ts",
  "package/dist/s3/post.js",
  "package/dist/s3/post.d.ts",
  "package/dist/transfers.js",
  "package/dist/transfers.d.ts",
  "package/dist/transfers/node.js",
  "package/dist/transfers/node.d.ts",
  "package/dist/s3/transfers.js",
  "package/dist/s3/transfers.d.ts",
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
  for (const required of [
    "LICENSE",
    "dist/index.js",
    "dist/index.d.ts",
    "dist/testing.js",
    "dist/testing.d.ts",
    "dist/s3.js",
    "dist/s3.d.ts",
    "dist/s3/signing.js",
    "dist/s3/signing.d.ts",
    "dist/s3/post.js",
    "dist/s3/post.d.ts",
    "dist/transfers.js",
    "dist/transfers.d.ts",
    "dist/transfers/node.js",
    "dist/transfers/node.d.ts",
    "dist/s3/transfers.js",
    "dist/s3/transfers.d.ts",
  ]) {
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
