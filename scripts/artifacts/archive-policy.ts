import { Schema } from "effect";

const Manifest = Schema.Struct({
  name: Schema.Literal("@effect-kit/storage"),
  version: Schema.NonEmptyString,
  type: Schema.Literal("module"),
  main: Schema.Literal("./dist/index.js"),
  types: Schema.Literal("./dist/index.d.ts"),
  peerDependencies: Schema.Struct({
    effect: Schema.NonEmptyString,
    "@aws-sdk/client-s3": Schema.NonEmptyString,
    "@aws-sdk/s3-request-presigner": Schema.NonEmptyString,
    "@aws-sdk/s3-presigned-post": Schema.NonEmptyString,
  }),
  peerDependenciesMeta: Schema.Struct({
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
    "./package.json": Schema.Literal("./package.json"),
    "./s3": Schema.Struct({
      types: Schema.Literal("./dist/s3.d.ts"),
      import: Schema.Literal("./dist/s3.js"),
    }),
    "./s3/signing": Schema.Struct({
      types: Schema.Literal("./dist/s3/signing.d.ts"),
      import: Schema.Literal("./dist/s3/signing.js"),
    }),
    "./s3/post": Schema.Struct({
      types: Schema.Literal("./dist/s3/post.d.ts"),
      import: Schema.Literal("./dist/s3/post.js"),
    }),
  }),
});

export function checkArchive(manifestInput: unknown, entries: readonly string[]): void {
  Schema.decodeUnknownSync(Manifest)(manifestInput);
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
    "dist/testing.js",
    "dist/testing.d.ts",
    "dist/s3.js",
    "dist/s3.d.ts",
    "dist/s3/signing.js",
    "dist/s3/signing.d.ts",
    "dist/s3/post.js",
    "dist/s3/post.d.ts",
  ]) {
    if (!entries.includes(`package/${required}`))
      throw new Error(`Missing packed file: ${required}`);
  }
}
