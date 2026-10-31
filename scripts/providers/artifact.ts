import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

export async function buildDigest() {
  const visit = async (directory: string): Promise<string[]> => {
    const entries = await readdir(directory, { withFileTypes: true });
    const nested = await Promise.all(
      entries.map((entry) => {
        if (entry.isSymbolicLink()) throw new Error("Symlink in build");
        const path = join(directory, entry.name);
        return entry.isDirectory() ? visit(path) : Promise.resolve([path]);
      }),
    );
    return nested.flat();
  };
  const paths = [
    ...(await visit("packages/storage/dist")),
    ...(await visit("packages/storage/test/e2e/live")),
    "pnpm-lock.yaml",
    "packages/storage/package.json",
  ].toSorted();
  const bytes = await Promise.all(paths.map(async (path) => [path, await readFile(path)] as const));
  const hash = createHash("sha256");
  for (const [path, content] of bytes) hash.update(path).update("\0").update(content).update("\0");
  return hash.digest("hex");
}
