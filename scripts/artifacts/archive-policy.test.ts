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
    "@effect/platform-node": "^4.1.0",
    "@effect/platform-bun": "^4.1.0",
    "@effect/platform-deno": "^4.1.0",
    "@effect/platform-browser": "^4.1.0",
    effect: "^4.1.0",
    "@aws-sdk/client-s3": "^3.1159.0",
    "@aws-sdk/s3-request-presigner": "^3.1159.0",
    "@aws-sdk/s3-presigned-post": "^3.1159.0",
  },
  peerDependenciesMeta: {
    "@effect/platform-browser": { optional: true },
    "@effect/platform-deno": { optional: true },
    "@effect/platform-bun": { optional: true },
    "@effect/platform-node": { optional: true },
    "@aws-sdk/client-s3": { optional: true },
    "@aws-sdk/s3-request-presigner": { optional: true },
    "@aws-sdk/s3-presigned-post": { optional: true },
  },
  exports: {
    ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
    "./testing": { types: "./dist/testing.d.ts", import: "./dist/testing.js" },
    "./node": { types: "./dist/node.d.ts", import: "./dist/node.js" },
    "./bun": { types: "./dist/bun.d.ts", import: "./dist/bun.js" },
    "./deno": { types: "./dist/deno.d.ts", import: "./dist/deno.js" },
    "./r2-workers": { types: "./dist/r2-workers.d.ts", import: "./dist/r2-workers.js" },
    "./filesystem": { types: "./dist/filesystem.d.ts", import: "./dist/filesystem.js" },
    "./browser": { types: "./dist/browser.d.ts", import: "./dist/browser.js" },
    "./package.json": "./package.json",
    "./s3": { types: "./dist/s3.d.ts", import: "./dist/s3.js" },
    "./s3/signing": { types: "./dist/s3/signing.d.ts", import: "./dist/s3/signing.js" },
    "./transfers": { types: "./dist/transfers.d.ts", import: "./dist/transfers.js" },
    "./sessions": { types: "./dist/sessions.d.ts", import: "./dist/sessions.js" },
    "./s3/post": { types: "./dist/s3/post.d.ts", import: "./dist/s3/post.js" },
  },
};
const entries = [
  "package/package.json",
  "package/README.md",
  "package/LICENSE",
  "package/dist/index.js",
  "package/dist/index.d.ts",
  "package/dist/node.js",
  "package/dist/node.d.ts",
  "package/dist/bun.js",
  "package/dist/bun.d.ts",
  "package/dist/deno.js",
  "package/dist/deno.d.ts",
  "package/dist/r2-workers.js",
  "package/dist/r2-workers.d.ts",
  "package/dist/filesystem.js",
  "package/dist/filesystem.d.ts",
  "package/dist/browser.js",
  "package/dist/browser.d.ts",
  "package/dist/testing.js",
  "package/dist/testing.d.ts",
  "package/dist/s3.js",
  "package/dist/s3.d.ts",
  "package/dist/s3/signing.js",
  "package/dist/s3/signing.d.ts",
  "package/dist/s3/post.js",
  "package/dist/s3/post.d.ts",
  "package/dist/sessions.js",
  "package/dist/sessions.d.ts",

  "package/dist/transfers.js",
  "package/dist/transfers.d.ts",
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
    "dist/node.js",
    "dist/node.d.ts",
    "dist/bun.js",
    "dist/bun.d.ts",
    "dist/deno.js",
    "dist/deno.d.ts",
    "dist/r2-workers.js",
    "dist/r2-workers.d.ts",
    "dist/filesystem.js",
    "dist/filesystem.d.ts",
    "dist/browser.js",
    "dist/browser.d.ts",
    "dist/testing.js",
    "dist/testing.d.ts",
    "dist/s3.js",
    "dist/s3.d.ts",
    "dist/s3/signing.js",
    "dist/s3/signing.d.ts",
    "dist/s3/post.js",
    "dist/s3/post.d.ts",
    "dist/sessions.js",
    "dist/sessions.d.ts",

    "dist/transfers.js",
    "dist/transfers.d.ts",
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
  for (const removed of ["./transfers/node", "./sessions/node", "./s3/transfers"]) {
    assert.throws(() =>
      checkArchive(
        {
          ...manifest,
          exports: { ...manifest.exports, [removed]: manifest.exports["./node"] },
        },
        entries,
      ),
    );
  }
});
