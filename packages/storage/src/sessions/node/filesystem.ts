import { createHash, randomUUID } from "node:crypto";
import { mkdir, open, readFile, readdir, rename, rm, stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { Effect } from "effect";
import { KeyValueStore } from "effect/persistence";

import { sessionError } from "../store.js";

export const fileEffect = <A>(action: () => Promise<A>) =>
  Effect.tryPromise({ try: action, catch: () => sessionError("SessionFileIO") });
export const syncDirectory = async (directory: string) => {
  const handle = await open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
};

/** Trusted private local directory; one application owner across processes. No lock/lease is implied. */
export const atomicFileStore = Effect.fnUntraced(function* (directory: string) {
  const root = resolve(directory);
  const created = yield* fileEffect(() => mkdir(root, { recursive: true, mode: 0o700 }));
  if (created) {
    const stop = dirname(resolve(created));
    let directoryToSync = root;
    while (true) {
      yield* fileEffect(() => syncDirectory(directoryToSync));
      if (directoryToSync === stop || dirname(directoryToSync) === directoryToSync) break;
      directoryToSync = dirname(directoryToSync);
    }
  }
  const pathFor = (key: string) =>
    join(root, `checkpoint-${createHash("sha256").update(key).digest("hex")}.json`);
  return KeyValueStore.makeStringOnly({
    // Hashed filenames cannot reconstruct logical keys. Recovery uses application-known IDs.
    keys: Effect.fail(
      new KeyValueStore.KeyValueStoreError({
        method: "keys",
        message: "Checkpoint backing does not support key enumeration",
      }),
    ),
    get: (key) =>
      fileEffect(async () => {
        try {
          if ((await stat(pathFor(key))).size > 64 * 1024 * 1024)
            throw new Error("Checkpoint too large");
          return await readFile(pathFor(key), "utf8");
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
          throw error;
        }
      }).pipe(
        Effect.mapError(
          () =>
            new KeyValueStore.KeyValueStoreError({
              method: "get",
              key: "checkpoint",
              message: "Checkpoint read failed",
            }),
        ),
      ),
    set: (key, value) =>
      fileEffect(async () => {
        const destination = pathFor(key);
        const temporary = join(root, `.pending-${randomUUID()}`);
        try {
          const handle = await open(temporary, "wx", 0o600);
          try {
            await handle.writeFile(value, "utf8");
            await handle.sync();
          } finally {
            await handle.close();
          }
          await rename(temporary, destination);
          await syncDirectory(root);
        } finally {
          await rm(temporary, { force: true });
        }
      }).pipe(
        Effect.mapError(
          () =>
            new KeyValueStore.KeyValueStoreError({
              method: "set",
              key: "checkpoint",
              message: "Checkpoint write failed",
            }),
        ),
      ),
    remove: (key) =>
      fileEffect(async () => {
        await rm(pathFor(key), { force: true });
        await syncDirectory(root);
      }).pipe(
        Effect.mapError(
          () =>
            new KeyValueStore.KeyValueStoreError({
              method: "remove",
              key: "checkpoint",
              message: "Checkpoint removal failed",
            }),
        ),
      ),
    clear: fileEffect(async () => {
      for (const name of await readdir(root))
        if (/^checkpoint-[a-f0-9]{64}\.json$/u.test(name)) await rm(join(root, name));
      await syncDirectory(root);
    }).pipe(
      Effect.mapError(
        () =>
          new KeyValueStore.KeyValueStoreError({
            method: "clear",
            key: "checkpoint",
            message: "Checkpoint clear failed",
          }),
      ),
    ),
    size: fileEffect(
      async () =>
        (await readdir(root)).filter((name) => /^checkpoint-[a-f0-9]{64}\.json$/u.test(name))
          .length,
    ).pipe(
      Effect.mapError(
        () =>
          new KeyValueStore.KeyValueStoreError({
            method: "size",
            key: "checkpoint",
            message: "Checkpoint inventory failed",
          }),
      ),
    ),
  });
});
