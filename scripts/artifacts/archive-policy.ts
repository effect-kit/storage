import { Schema } from "effect";

const Manifest = Schema.Struct({
  name: Schema.Literal("@effect-kit/storage"),
  version: Schema.NonEmptyString,
  type: Schema.Literal("module"),
  main: Schema.Literal("./dist/index.js"),
  types: Schema.Literal("./dist/index.d.ts"),
  peerDependencies: Schema.Struct({ effect: Schema.NonEmptyString }),
  exports: Schema.Struct({
    ".": Schema.Struct({
      types: Schema.Literal("./dist/index.d.ts"),
      import: Schema.Literal("./dist/index.js"),
    }),
    "./package.json": Schema.Literal("./package.json"),
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
  ]) {
    if (!entries.includes(`package/${required}`))
      throw new Error(`Missing packed file: ${required}`);
  }
}
