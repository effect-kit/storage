/* oxlint-disable no-await-in-loop -- Registry bytes are bounded while streamed in order. */
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Schema } from "effect";

import type { Candidate } from "./plan.ts";
import { integrity } from "./plan.ts";

const Metadata = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  dist: Schema.Struct({ tarball: Schema.String, integrity: Schema.String }),
});
async function bounded(url: string, limit: number) {
  const parsed = URL.parse(url);
  if (
    !parsed ||
    parsed.origin !== "https://registry.npmjs.org" ||
    parsed.username ||
    parsed.password ||
    parsed.hash
  )
    throw new Error("Untrusted npm registry URL");
  const response = await fetch(url, { redirect: "error", signal: AbortSignal.timeout(30000) });
  if (!response.ok || !response.body) throw new Error("Registry object unavailable");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      length += next.value.length;
      if (length > limit) throw new Error("Registry object exceeds limit");
      chunks.push(next.value);
    }
  } finally {
    await reader.cancel();
  }
  return Buffer.concat(chunks);
}
export function matchRegistry(
  input: unknown,
  bytes: Uint8Array,
  entry: Candidate["packages"][number],
) {
  const metadata = Schema.decodeUnknownSync(Metadata)(input);
  if (
    metadata.name !== entry.name ||
    metadata.version !== entry.version ||
    metadata.dist.integrity !== entry.digest ||
    integrity(bytes, "sha512") !== entry.digest
  )
    throw new Error("Registry archive differs from verified release candidate");
  return metadata;
}
export async function downloadRegistry(entry: Candidate["packages"][number], provenance: boolean) {
  const metadata = Schema.decodeUnknownSync(Metadata)(
    JSON.parse(
      (
        await bounded(
          `https://registry.npmjs.org/${encodeURIComponent(entry.name)}/${entry.version}`,
          4 * 1024 * 1024,
        )
      ).toString(),
    ),
  );
  const bytes = await bounded(metadata.dist.tarball, 32 * 1024 * 1024);
  matchRegistry(metadata, bytes, entry);
  let audit: unknown;
  if (provenance) {
    const directory = await mkdtemp(join(tmpdir(), "storage-provenance-"));
    try {
      await writeFile(
        join(directory, "package.json"),
        JSON.stringify({ private: true, dependencies: { [entry.name]: entry.version } }),
      );
      const run = (args: string[]) =>
        execFileSync("npm", args, {
          cwd: directory,
          stdio: "pipe",
          encoding: "utf8",
          timeout: 180000,
          maxBuffer: 16 * 1024 * 1024,
        });
      run([
        "install",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        "--legacy-peer-deps",
        "--registry=https://registry.npmjs.org/",
      ]);
      audit = JSON.parse(
        run([
          "audit",
          "signatures",
          "--json",
          "--include-attestations",
          "--registry=https://registry.npmjs.org/",
        ]),
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
  return { metadata, bytes, audit };
}
