import { ByteSize, Effect, FileSystem, Path, Schema } from "effect";
import { Digest, SecureRandom } from "effect/crypto";
import type { PlatformError } from "effect/PlatformError";

import { storageError } from "../../core/errors.js";
import type { MutationOutcome } from "../../core/errors.js";
import { syncDirectory } from "../../runtime/filesystem.js";
import { TransferHash } from "../../transfers/dependencies.js";
import { initializeIdentity } from "./identity.js";

export const FilesystemOptions = Schema.Struct({
  root: Schema.NonEmptyString,
  storeId: Schema.NonEmptyString,
  maxObjectBytes: Schema.optional(
    Schema.Natural.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER - 65536)),
  ),
  maxListEntries: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000000 })),
  ),
});
export type FilesystemOptions = typeof FilesystemOptions.Type;

export const fileError = (
  operation: string,
  code: string,
  outcome: MutationOutcome = "not-applicable",
) => storageError(operation, "filesystem", { _tag: "ProviderFailure", code }, outcome);

export const mapIO =
  (operation: string, outcome: MutationOutcome = "not-applicable") =>
  (error: PlatformError) => {
    const reason = error.reason._tag;
    return storageError(
      operation,
      "filesystem",
      reason === "NotFound"
        ? { _tag: "NotFound" }
        : reason === "PermissionDenied"
          ? { _tag: "AccessDenied" }
          : { _tag: "ProviderFailure", code: reason },
      outcome,
    );
  };

export const makeLayout = Effect.fnUntraced(function* (options: FilesystemOptions) {
  const input = yield* Schema.decodeUnknownEffect(FilesystemOptions, { onExcessProperty: "error" })(
    options,
  ).pipe(
    Effect.mapError(() =>
      storageError("make", "filesystem", { _tag: "InvalidConfiguration", field: "options" }),
    ),
  );
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const identities = yield* Digest.Digest;
  const random = yield* SecureRandom.SecureRandom;
  const hash = yield* TransferHash;
  if (hash.algorithm !== "sha256")
    return yield* storageError("make", "filesystem", {
      _tag: "InvalidConfiguration",
      field: "TransferHash.algorithm",
    });

  const assertDirectory = Effect.fnUntraced(function* (directory: string) {
    const info = yield* fs.lstat(directory);
    if (info.type !== "Directory")
      return yield* fileError("filesystem.directory", "UnsafeDirectory");
  });
  const suppliedRoot = path.resolve(input.root);
  if (yield* fs.exists(suppliedRoot)) yield* assertDirectory(suppliedRoot);
  const missing: string[] = [];
  let ancestor = suppliedRoot;
  while (!(yield* fs.exists(ancestor))) {
    missing.push(ancestor);
    if (path.dirname(ancestor) === ancestor) break;
    ancestor = path.dirname(ancestor);
  }
  yield* fs
    .makeDirectory(suppliedRoot, { recursive: true, mode: 0o700 })
    .pipe(Effect.mapError(mapIO("make")));
  for (const created of missing)
    yield* syncDirectory(fs, created).pipe(Effect.mapError(mapIO("make")));
  if (missing.length) yield* syncDirectory(fs, ancestor).pipe(Effect.mapError(mapIO("make")));
  const root = yield* fs.realPath(suppliedRoot).pipe(Effect.mapError(mapIO("make")));
  const objects = path.join(root, "objects");
  const staging = path.join(root, "staging");
  for (const directory of [objects, staging]) {
    yield* fs.makeDirectory(directory, { mode: 0o700 }).pipe(
      Effect.catchReason("PlatformError", "AlreadyExists", () => Effect.void),
      Effect.mapError(mapIO("make")),
    );
    yield* assertDirectory(directory).pipe(
      Effect.mapError((error) => (error._tag === "PlatformError" ? mapIO("make")(error) : error)),
    );
  }
  yield* syncDirectory(fs, root).pipe(Effect.mapError(mapIO("make")));

  const rootId = yield* initializeIdentity(fs, path, root, input.storeId, random);

  const checkDirectories = Effect.all(
    [assertDirectory(root), assertDirectory(objects), assertDirectory(staging)],
    { discard: true },
  ).pipe(
    Effect.mapError((error) =>
      error._tag === "PlatformError" ? mapIO("filesystem.directory")(error) : error,
    ),
  );
  const objectFilename = (key: string) =>
    identities
      .digestHex(Digest.sha256, new TextEncoder().encode(key))
      .pipe(Effect.map((digest) => path.join(objects, `${digest}.object`)));
  const regularFile = Effect.fnUntraced(function* (filename: string) {
    const info = yield* fs.lstat(filename);
    if (info.type !== "File") return yield* fileError("filesystem.open", "UnsafeObject");
    return info;
  });
  const open = Effect.fnUntraced(function* (filename: string) {
    yield* checkDirectories;
    yield* regularFile(filename);
    const file = yield* fs.open(filename, { flag: "r", noFollow: true });
    if ((yield* file.stat).type !== "File")
      return yield* fileError("filesystem.open", "UnsafeObject");
    return file;
  });

  return {
    fs,
    path,
    root,
    rootId,
    objects,
    staging,
    storeId: input.storeId,
    random,
    hash,
    filename: objectFilename,
    open,
    regularFile,
    checkDirectories,
    maxObjectBytes: input.maxObjectBytes ?? 1024 * 1024 * 1024,
    maxListEntries: input.maxListEntries ?? 10000,
  };
});
export type FilesystemContext = Effect.Success<ReturnType<typeof makeLayout>>;
export const fileSize = (info: FileSystem.File.Info) => ByteSize.toBigInt(info.size);
