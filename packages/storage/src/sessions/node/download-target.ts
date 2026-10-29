import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { open, rename, rm, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import { Effect, Stream } from "effect";

import type { StorageError } from "../../errors.js";
import type { Checksum } from "../../object.js";
import { sha256 } from "../../transfers/node.js";
import { sameChecksum } from "../../transfers/reader.js";
import type { RestartableDownloadTarget } from "../download.js";
import { sessionError } from "../store.js";
import { fileEffect, syncDirectory } from "./filesystem.js";

// Node filesystem promises cannot cancel an in-flight write. Keep ownership until it finishes.
const mutateFile = <A>(action: () => Promise<A>) => fileEffect(action).pipe(Effect.uninterruptible);

const hashFile = async (path: string, expected: Checksum, length: number) => {
  if ((await stat(path)).size !== length) throw new Error("Length mismatch");
  const digest = sha256.create();
  for await (const chunk of createReadStream(path, { highWaterMark: 65536 })) digest.update(chunk);
  if (!sameChecksum(digest.finish(), expected)) throw new Error("Checksum mismatch");
};

export function fileDownloadTarget(options: {
  readonly id: string;
  readonly path: string;
}): RestartableDownloadTarget<StorageError> {
  const destination = resolve(options.path);
  if (!/^[A-Za-z0-9_-]{1,100}$/u.test(options.id))
    throw new TypeError("Invalid download target id");
  const partial = `${destination}.${options.id}.partial`;
  return {
    id: options.id,
    identity: createHash("sha256")
      .update(`${options.id}\0${destination}\0${partial}`)
      .digest("hex"),
    discard: () =>
      mutateFile(async () => {
        await rm(partial, { force: true });
        await syncDirectory(dirname(destination));
      }),
    prefix: (length) =>
      length === 0
        ? Stream.empty
        : Stream.fromAsyncIterable(
            createReadStream(partial, { start: 0, end: length - 1, highWaterMark: 65536 }),
            () => sessionError("PartialRead"),
          ),
    truncate: (length) =>
      mutateFile(async () => {
        const handle = await open(partial, length === 0 ? "a+" : "r+", 0o600);
        try {
          if ((await handle.stat()).size < length)
            throw new Error("Partial shorter than checkpoint");
          await handle.truncate(length);
          await handle.sync();
        } finally {
          await handle.close();
        }
        await syncDirectory(dirname(destination));
      }),
    write: (offset, bytes) =>
      mutateFile(async () => {
        const handle = await open(partial, "r+");
        try {
          let written = 0;
          while (written < bytes.length) {
            // Positional short writes must finish in order before the checkpoint can advance.
            // eslint-disable-next-line no-await-in-loop
            const result = await handle.write(
              bytes,
              written,
              bytes.length - written,
              offset + written,
            );
            if (result.bytesWritten === 0) throw new Error("Zero-byte write");
            written += result.bytesWritten;
          }
        } finally {
          await handle.close();
        }
      }),
    sync: mutateFile(async () => {
      const handle = await open(partial, "r+");
      try {
        await handle.sync();
      } finally {
        await handle.close();
      }
      await syncDirectory(dirname(destination));
    }),
    publish: (checksum, length) =>
      mutateFile(async () => {
        try {
          await stat(partial);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
          await hashFile(destination, checksum, length);
          return;
        }
        await hashFile(partial, checksum, length);
        await rename(partial, destination);
        await syncDirectory(dirname(destination));
      }),
  };
}
