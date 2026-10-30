import { Schema } from "effect";

const Manifest = Schema.Struct({
  name: Schema.Literal("@effect-kit/storage"),
  version: Schema.NonEmptyString,
  type: Schema.Literal("module"),
  main: Schema.Literal("./dist/index.js"),
  types: Schema.Literal("./dist/index.d.ts"),
  peerDependencies: Schema.Struct({
    "@effect/platform-node": Schema.NonEmptyString,
    "@effect/platform-bun": Schema.NonEmptyString,
    "@effect/platform-deno": Schema.NonEmptyString,
    "@effect/platform-browser": Schema.NonEmptyString,
    effect: Schema.NonEmptyString,
    "@google-cloud/storage": Schema.NonEmptyString,
    "@azure/storage-blob": Schema.NonEmptyString,
    "@azure/identity": Schema.NonEmptyString,
    "@aws-sdk/client-s3": Schema.NonEmptyString,
    "@aws-sdk/s3-request-presigner": Schema.NonEmptyString,
    "@aws-sdk/s3-presigned-post": Schema.NonEmptyString,
  }),
  peerDependenciesMeta: Schema.Struct({
    "@effect/platform-browser": Schema.Struct({ optional: Schema.Literal(true) }),
    "@effect/platform-deno": Schema.Struct({ optional: Schema.Literal(true) }),
    "@effect/platform-bun": Schema.Struct({ optional: Schema.Literal(true) }),
    "@effect/platform-node": Schema.Struct({ optional: Schema.Literal(true) }),
    "@google-cloud/storage": Schema.Struct({ optional: Schema.Literal(true) }),
    "@azure/storage-blob": Schema.Struct({ optional: Schema.Literal(true) }),
    "@azure/identity": Schema.Struct({ optional: Schema.Literal(true) }),
    "@aws-sdk/client-s3": Schema.Struct({ optional: Schema.Literal(true) }),
    "@aws-sdk/s3-request-presigner": Schema.Struct({ optional: Schema.Literal(true) }),
    "@aws-sdk/s3-presigned-post": Schema.Struct({ optional: Schema.Literal(true) }),
  }),
  exports: Schema.Struct({
    ".": Schema.Struct({
      types: Schema.Literal("./dist/index.d.ts"),
      import: Schema.Literal("./dist/index.js"),
    }),
    "./testing": Schema.Struct({
      types: Schema.Literal("./dist/testing.d.ts"),
      import: Schema.Literal("./dist/testing.js"),
    }),
    "./node": Schema.Struct({
      types: Schema.Literal("./dist/node.d.ts"),
      import: Schema.Literal("./dist/node.js"),
    }),
    "./bun": Schema.Struct({
      types: Schema.Literal("./dist/bun.d.ts"),
      import: Schema.Literal("./dist/bun.js"),
    }),
    "./deno": Schema.Struct({
      types: Schema.Literal("./dist/deno.d.ts"),
      import: Schema.Literal("./dist/deno.js"),
    }),
    "./r2-workers": Schema.Struct({
      types: Schema.Literal("./dist/r2-workers.d.ts"),
      import: Schema.Literal("./dist/r2-workers.js"),
    }),
    "./filesystem": Schema.Struct({
      types: Schema.Literal("./dist/filesystem.d.ts"),
      import: Schema.Literal("./dist/filesystem.js"),
    }),
    "./browser": Schema.Struct({
      types: Schema.Literal("./dist/browser.d.ts"),
      import: Schema.Literal("./dist/browser.js"),
    }),
    "./package.json": Schema.Literal("./package.json"),
    "./gcs": Schema.Struct({
      types: Schema.Literal("./dist/gcs.d.ts"),
      import: Schema.Literal("./dist/gcs.js"),
    }),
    "./gcs/signing": Schema.Struct({
      types: Schema.Literal("./dist/gcs/signing.d.ts"),
      import: Schema.Literal("./dist/gcs/signing.js"),
    }),
    "./gcs/post": Schema.Struct({
      types: Schema.Literal("./dist/gcs/post.d.ts"),
      import: Schema.Literal("./dist/gcs/post.js"),
    }),
    "./azure": Schema.Struct({
      types: Schema.Literal("./dist/azure.d.ts"),
      import: Schema.Literal("./dist/azure.js"),
    }),
    "./azure/signing": Schema.Struct({
      types: Schema.Literal("./dist/azure/signing.d.ts"),
      import: Schema.Literal("./dist/azure/signing.js"),
    }),
    "./azure/identity": Schema.Struct({
      types: Schema.Literal("./dist/azure/identity.d.ts"),
      import: Schema.Literal("./dist/azure/identity.js"),
    }),
    "./s3": Schema.Struct({
      types: Schema.Literal("./dist/s3.d.ts"),
      import: Schema.Literal("./dist/s3.js"),
    }),
    "./s3/signing": Schema.Struct({
      types: Schema.Literal("./dist/s3/signing.d.ts"),
      import: Schema.Literal("./dist/s3/signing.js"),
    }),
    "./transfers": Schema.Struct({
      types: Schema.Literal("./dist/transfers.d.ts"),
      import: Schema.Literal("./dist/transfers.js"),
    }),
    "./sessions": Schema.Struct({
      types: Schema.Literal("./dist/sessions.d.ts"),
      import: Schema.Literal("./dist/sessions.js"),
    }),
    "./s3/post": Schema.Struct({
      types: Schema.Literal("./dist/s3/post.d.ts"),
      import: Schema.Literal("./dist/s3/post.js"),
    }),
  }),
});

export function checkArchive(manifestInput: unknown, entries: readonly string[]): void {
  Schema.decodeUnknownSync(Manifest)(manifestInput);
  for (const old of ["./transfers/node", "./sessions/node", "./s3/transfers"]) {
    if (Object.hasOwn((manifestInput as { exports: object }).exports, old))
      throw new Error(`Removed export remains: ${old}`);
  }
  if (JSON.stringify(manifestInput).includes("workspace-source")) {
    throw new Error("Archive contains development export conditions");
  }

  for (const entry of entries) {
    if (
      !/^package\/(package\.json|README\.md|CHANGELOG\.md|LICENSE|dist\/[^.][^\\]*)$/.test(entry) ||
      entry.includes("\\") ||
      entry.split("/").some((part) => part.startsWith("."))
    ) {
      throw new Error(`Unexpected packed path: ${entry}`);
    }
  }

  for (const required of [
    "package.json",
    "README.md",
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
    "dist/gcs.js",
    "dist/gcs.d.ts",
    "dist/gcs/signing.js",
    "dist/gcs/signing.d.ts",
    "dist/gcs/post.js",
    "dist/gcs/post.d.ts",
    "dist/azure.js",
    "dist/azure.d.ts",
    "dist/azure/signing.js",
    "dist/azure/signing.d.ts",
    "dist/azure/identity.js",
    "dist/azure/identity.d.ts",
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
    if (!entries.includes(`package/${required}`))
      throw new Error(`Missing packed file: ${required}`);
  }
}
