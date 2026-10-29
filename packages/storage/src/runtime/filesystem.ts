import { ByteSize, Effect, Exit, FileSystem, Path } from "effect";
import { Digest, SecureRandom } from "effect/crypto";
import { KeyValueStore } from "effect/persistence";

export const syncDirectory = (fs: FileSystem.FileSystem, directory: string) =>
  Effect.scoped(Effect.flatMap(fs.open(directory, { flag: "r" }), (handle) => handle.sync));

const ioError = (method: string) =>
  new KeyValueStore.KeyValueStoreError({
    method,
    key: "checkpoint",
    message: `Checkpoint ${method} failed`,
  });

/** Private trusted root; the application owns cross-process exclusion. */
export const atomicFileStore = Effect.fnUntraced(function* (directory: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const digest = yield* Digest.Digest;
  const random = yield* SecureRandom.SecureRandom;
  const root = path.resolve(directory);

  yield* Effect.gen(function* () {
    const missing: string[] = [];
    let ancestor = root;
    while (!(yield* fs.exists(ancestor))) {
      missing.push(ancestor);
      if (path.dirname(ancestor) === ancestor) break;
      ancestor = path.dirname(ancestor);
    }
    yield* fs.makeDirectory(root, { recursive: true, mode: 0o700 });
    for (const created of missing) yield* syncDirectory(fs, created);
    if (missing.length) yield* syncDirectory(fs, ancestor);
  }).pipe(
    Effect.uninterruptible,
    Effect.mapError(() => ioError("make")),
  );

  const pathFor = (key: string) =>
    Effect.map(digest.digestHex(Digest.sha256, new TextEncoder().encode(key)), (hash) =>
      path.join(root, `checkpoint-${hash}.json`),
    );
  const filenames = fs
    .readDirectory(root)
    .pipe(
      Effect.map((names) => names.filter((name) => /^checkpoint-[a-f0-9]{64}\.json$/u.test(name))),
    );
  return KeyValueStore.makeStringOnly({
    // Hash filenames cannot reconstruct logical keys. Recovery uses known session IDs.
    keys: Effect.fail(ioError("keys")),
    get: (key) =>
      Effect.gen(function* () {
        const filename = yield* pathFor(key);
        const info = yield* fs.stat(filename);
        if (ByteSize.toBigInt(info.size) > 64n * 1024n * 1024n) return yield* ioError("get");
        return yield* fs.readFileString(filename);
      }).pipe(
        Effect.catchReason("PlatformError", "NotFound", () => Effect.undefined),
        Effect.mapError(() => ioError("get")),
      ),
    set: (key, value) =>
      Effect.gen(function* () {
        const temporary = path.join(root, `.pending-${yield* random.uuidV4()}`);
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* fs.open(temporary, { flag: "wx", mode: 0o600 });
            yield* handle.writeAll(new TextEncoder().encode(value));
            yield* handle.sync;
          }),
        )
          .pipe(
            Effect.andThen(
              Effect.flatMap(pathFor(key), (destination) => fs.rename(temporary, destination)),
            ),
            Effect.andThen(syncDirectory(fs, root)),
          )
          .pipe(Effect.exit);
        const cleanup = yield* fs.remove(temporary, { force: true }).pipe(Effect.result);
        if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause);
        yield* Effect.fromResult(cleanup);
      }).pipe(
        Effect.uninterruptible,
        Effect.mapError(() => ioError("set")),
      ),
    remove: (key) =>
      pathFor(key).pipe(
        Effect.flatMap((filename) => fs.remove(filename, { force: true })),
        Effect.andThen(syncDirectory(fs, root)),
        Effect.uninterruptible,
        Effect.mapError(() => ioError("remove")),
      ),
    clear: Effect.gen(function* () {
      for (const name of yield* filenames) yield* fs.remove(path.join(root, name));
      yield* syncDirectory(fs, root);
    }).pipe(
      Effect.uninterruptible,
      Effect.mapError(() => ioError("clear")),
    ),
    size: filenames.pipe(
      Effect.map((names) => names.length),
      Effect.mapError(() => ioError("size")),
    ),
  });
});
