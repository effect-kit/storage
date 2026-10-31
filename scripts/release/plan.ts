/* oxlint-disable no-await-in-loop -- Bounded archive inspection is sequential to limit decompressed memory. */
import { createHash } from "node:crypto";
import { lstat, readFile, readdir, realpath } from "node:fs/promises";
import { join, resolve, sep } from "node:path";

import { Schema } from "effect";

import { checkArchive } from "../artifacts/archive-policy.ts";
import { inspectArchive } from "./archive.ts";

const Version = Schema.String.check(
  Schema.isPattern(/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/u),
);
const Name = Schema.String.check(Schema.isPattern(/^(?:@[a-z0-9._-]+\/)?[a-z0-9][a-z0-9._-]*$/u));
const Release = Schema.Struct({
  kind: Schema.Literal("publish"),
  name: Name,
  version: Version,
  access: Schema.Literal("public"),
  tag: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]*$/u)),
  tarball: Schema.Struct({
    path: Schema.NonEmptyString,
    integrity: Schema.String.check(Schema.isPattern(/^sha256-[A-Za-z0-9+/]{43}=$/u)),
  }),
});
const Plan = Schema.Struct({
  version: Schema.Literal(1),
  plan: Schema.Array(Schema.Array(Release)),
});
export type ReleaseEntry = typeof Release.Type;
export const integrity = (bytes: Uint8Array, algorithm: "sha256" | "sha512" = "sha256") =>
  `${algorithm}-${createHash(algorithm).update(bytes).digest("base64")}`;

export function parsePlan(input: unknown, expected: ReadonlyMap<string, string>) {
  const plan = Schema.decodeUnknownSync(Plan, { onExcessProperty: "error" })(input);
  const entries = plan.plan.flat();
  if (
    !entries.length ||
    entries.length > 32 ||
    new Set(entries.map((entry) => entry.name)).size !== entries.length
  )
    throw new Error("Empty or duplicate release plan");
  for (const entry of entries) {
    const filename = `${entry.name.replace(/^@/u, "").replaceAll("/", "-")}-${entry.version}.tgz`;
    if (expected.get(entry.name) !== entry.version || entry.tarball.path !== `packages/${filename}`)
      throw new Error("Unreviewed package/version or unsafe archive mapping");
  }
  return entries;
}
export async function workspacePackages() {
  const manifests = await Promise.all(
    (await readdir("packages", { withFileTypes: true }))
      .filter((entry) => entry.isDirectory())
      .map(async (entry) => {
        const manifest = JSON.parse(
          await readFile(join("packages", entry.name, "package.json"), "utf8"),
        ) as { name: string; version: string; private?: boolean };
        return manifest.private ? [] : [[manifest.name, manifest.version] as const];
      }),
  );
  const entries = manifests.flat();
  if (new Set(entries.map(([name]) => name)).size !== entries.length)
    throw new Error("Duplicate publishable workspace package");
  return new Map(entries);
}
export async function readCandidate(directory: string, expected?: ReadonlyMap<string, string>) {
  const root = await realpath(resolve(directory));
  const planPath = join(root, "publish-plan.json");
  if (!(await lstat(planPath)).isFile()) throw new Error("Plan must be a regular file");
  const planBytes = await readFile(planPath);
  if (planBytes.length > 1024 * 1024) throw new Error("Oversized release plan");
  const entries = parsePlan(
    JSON.parse(planBytes.toString()),
    expected ?? (await workspacePackages()),
  );
  const packageDirectory = join(root, "packages");
  if ((await lstat(packageDirectory)).isSymbolicLink())
    throw new Error("Archive directory is a symlink");
  const actual = (await readdir(packageDirectory)).toSorted();
  const planned = entries.map((entry) => entry.tarball.path.slice("packages/".length)).toSorted();
  if (JSON.stringify(actual) !== JSON.stringify(planned))
    throw new Error("Extra or missing release archives");
  const sizes = await Promise.all(
    entries.map(async (entry) => (await lstat(join(root, entry.tarball.path))).size),
  );
  if (sizes.reduce((sum, size) => sum + size, 0) > 64 * 1024 * 1024)
    throw new Error("Aggregate archive size exceeds limit");
  const packages = [];
  for (const entry of entries) {
    const path = join(root, entry.tarball.path);
    const stat = await lstat(path);
    if (
      !stat.isFile() ||
      stat.size > 32 * 1024 * 1024 ||
      !(await realpath(path)).startsWith(`${root}${sep}`)
    )
      throw new Error("Unsafe archive file");
    const bytes = await readFile(path);
    if (integrity(bytes) !== entry.tarball.integrity)
      throw new Error("Release archive integrity mismatch");
    const files = inspectArchive(bytes);
    const encoded = files.get("package/package.json");
    if (!encoded || encoded.length > 1024 * 1024)
      throw new Error("Missing/oversized packed manifest");
    const manifest = JSON.parse(new TextDecoder().decode(encoded)) as Record<string, unknown>;
    if (
      manifest.name !== entry.name ||
      manifest.version !== entry.version ||
      manifest.private === true ||
      manifest.type !== "module" ||
      JSON.stringify(manifest).includes("workspace-source")
    )
      throw new Error("Packed identity or exports mismatch");
    if (entry.name === "@effect-kit/storage") checkArchive(manifest, [...files.keys()]);
    for (const name of files.keys())
      if (
        !/^package\/(?:package\.json|README\.md|CHANGELOG\.md|LICENSE|dist\/[^.][^\\]*)$/u.test(
          name,
        )
      )
        throw new Error("Unexpected archive contents");
    if (
      new TextDecoder().decode(files.get("package/LICENSE")) !== (await readFile("LICENSE", "utf8"))
    )
      throw new Error("Release license mismatch");
    packages.push({ ...entry, archive: path, manifest, digest: integrity(bytes, "sha512"), bytes });
  }
  return { root, planDigest: integrity(planBytes), packages };
}
export type Candidate = Awaited<ReturnType<typeof readCandidate>>;
export async function unchanged(candidate: Candidate) {
  if (integrity(await readFile(join(candidate.root, "publish-plan.json"))) !== candidate.planDigest)
    throw new Error("Plan changed during verification");
  const entries = await readdir(join(candidate.root, "packages"));
  if (entries.length !== candidate.packages.length)
    throw new Error("Archive set changed during verification");
  for (const entry of candidate.packages)
    if (integrity(await readFile(entry.archive)) !== entry.tarball.integrity)
      throw new Error("Archive changed during verification");
}
