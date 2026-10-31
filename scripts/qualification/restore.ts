import { execFileSync } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const directory = await mkdtemp(join(tmpdir(), "storage-restore-"));
const invoke = (phase: string) =>
  execFileSync(
    process.execPath,
    ["packages/storage/test/provider/qualification/restore.ts", phase, directory],
    { stdio: "pipe", timeout: 60000 },
  );
try {
  invoke("seed");
  await cp(join(directory, "source"), join(directory, "backup"), { recursive: true });
  await rm(join(directory, "source"), { recursive: true });
  invoke("restore");
  process.stdout.write(
    "Fresh-process gzip/age restore verified from a disk copy after removing the source.\n",
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
