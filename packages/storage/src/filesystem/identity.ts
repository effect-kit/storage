import { ByteSize, Effect, Schema } from "effect";
import type { FileSystem, Path } from "effect";
import type { SecureRandom } from "effect/crypto";

import { storageError } from "../errors.js";
import { syncDirectory } from "../runtime/filesystem.js";

const Identity = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Literal(1),
    storeId: Schema.NonEmptyString,
    id: Schema.NonEmptyString,
  }),
);

/** A root is bound to one format/store identity; reopening with another identity fails closed. */
const invalid = () =>
  storageError("make", "filesystem", { _tag: "InvalidConfiguration", field: "root.identity" });

export const initializeIdentity = Effect.fnUntraced(function* (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  root: string,
  storeId: string,
  random: SecureRandom.SecureRandom["Service"],
) {
  const filename = path.join(root, "format.json");
  if (!(yield* fs.exists(filename))) {
    const names = yield* fs.readDirectory(root);
    if (
      names.some((name) => name !== "objects" && name !== "staging" && name !== "format.json") ||
      (yield* fs.readDirectory(path.join(root, "objects"))).length
    ) {
      if (!(yield* fs.exists(filename))) return yield* invalid();
    }
    const temporary = path.join(root, "staging", `upload-${yield* random.uuidV4()}.pending`);
    const encoded = Schema.encodeSync(Identity)({
      version: 1,
      storeId,
      id: yield* random.uuidV4(),
    });
    yield* Effect.acquireUseRelease(
      Effect.succeed(temporary),
      () =>
        Effect.gen(function* () {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const file = yield* fs.open(temporary, { flag: "wx", mode: 0o600, noFollow: true });
              yield* file.writeAll(new TextEncoder().encode(encoded));
              yield* file.sync;
            }),
          );
          yield* fs
            .link(temporary, filename)
            .pipe(Effect.catchReason("PlatformError", "AlreadyExists", () => Effect.void));
          yield* syncDirectory(fs, root);
        }),
      () => fs.remove(temporary, { force: true }).pipe(Effect.orDie),
    ).pipe(Effect.uninterruptible);
  }
  const identity = yield* Effect.scoped(
    Effect.gen(function* () {
      if ((yield* fs.lstat(filename)).type !== "File") return yield* invalid();
      const file = yield* fs.open(filename, { flag: "r", noFollow: true });
      const length = Number(ByteSize.toBigInt((yield* file.stat).size));
      if (length < 1 || length > 16384) return yield* invalid();
      const bytes = new Uint8Array(length);
      let offset = 0;
      while (offset < length) {
        const count = yield* file.read(bytes.subarray(offset));
        if (count === 0) return yield* invalid();
        offset += count;
      }
      const text = yield* Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        catch: invalid,
      });
      return yield* Schema.decodeUnknownEffect(Identity)(text).pipe(Effect.mapError(invalid));
    }),
  );
  if (identity.storeId !== storeId) return yield* invalid();
  return identity.id;
});
