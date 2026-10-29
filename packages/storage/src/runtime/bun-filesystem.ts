import { constants } from "node:fs";
import type { BigIntStats } from "node:fs";
import { open } from "node:fs/promises";

import * as BunFileSystem from "@effect/platform-bun/BunFileSystem";
import { ByteSize, Effect, FileSystem, Layer, Option, Schema } from "effect";
import { badArgument, systemError } from "effect/PlatformError";
import type { SystemErrorTag } from "effect/PlatformError";

const Errno = Schema.Struct({ code: Schema.optional(Schema.String) });
const categories: Readonly<Record<string, SystemErrorTag>> = {
  ENOENT: "NotFound",
  EEXIST: "AlreadyExists",
  EACCES: "PermissionDenied",
  EPERM: "PermissionDenied",
  EBADF: "BadResource",
  EBUSY: "Busy",
  EINVAL: "InvalidData",
  EAGAIN: "WouldBlock",
};
const io = <A>(method: string, run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (error) => {
      const decoded = Schema.decodeUnknownOption(Errno)(error);
      const code = Option.isSome(decoded) ? decoded.value.code : undefined;
      return systemError({
        module: "FileSystem",
        method,
        _tag: code ? (categories[code] ?? "Unknown") : "Unknown",
      });
    },
  }).pipe(Effect.uninterruptible);

const positionNumber = (position: bigint) =>
  position >= 0n && position <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Effect.succeed(Number(position))
    : Effect.fail(
        badArgument({
          module: "FileSystem",
          method: "position",
          description: "Unsupported file offset",
        }),
      );

const info = (stat: BigIntStats): FileSystem.File.Info => ({
  type: stat.isFile()
    ? "File"
    : stat.isDirectory()
      ? "Directory"
      : stat.isSymbolicLink()
        ? "SymbolicLink"
        : stat.isBlockDevice()
          ? "BlockDevice"
          : stat.isCharacterDevice()
            ? "CharacterDevice"
            : stat.isFIFO()
              ? "FIFO"
              : stat.isSocket()
                ? "Socket"
                : "Unknown",
  mtime: Option.some(stat.mtime),
  atime: Option.some(stat.atime),
  birthtime: Option.some(stat.birthtime),
  dev: BigInt.asUintN(64, stat.dev),
  ino: Option.some(BigInt.asUintN(64, stat.ino)),
  mode: Number(stat.mode),
  nlink: Option.some(BigInt.asUintN(64, stat.nlink)),
  uid: Option.some(Number(stat.uid)),
  gid: Option.some(Number(stat.gid)),
  rdev: Option.some(BigInt.asUintN(64, stat.rdev)),
  size: ByteSize.bytes(stat.size),
  blksize: Option.some(ByteSize.bytes(stat.blksize)),
  blocks: Option.some(stat.blocks),
});
const flags: Readonly<Record<FileSystem.OpenFlag, number>> = {
  r: constants.O_RDONLY,
  "r+": constants.O_RDWR,
  w: constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC,
  "w+": constants.O_RDWR | constants.O_CREAT | constants.O_TRUNC,
  wx: constants.O_WRONLY | constants.O_CREAT | constants.O_TRUNC | constants.O_EXCL,
  "wx+": constants.O_RDWR | constants.O_CREAT | constants.O_TRUNC | constants.O_EXCL,
  a: constants.O_WRONLY | constants.O_CREAT | constants.O_APPEND,
  "a+": constants.O_RDWR | constants.O_CREAT | constants.O_APPEND,
  ax: constants.O_WRONLY | constants.O_CREAT | constants.O_APPEND | constants.O_EXCL,
  "ax+": constants.O_RDWR | constants.O_CREAT | constants.O_APPEND | constants.O_EXCL,
};

// Bun 1.4.4 ignores Effect's undefined-offset write overload and bigint read positions.
// Explicit offset/length/number calls preserve the public FileSystem contract without patching globals.
const openFile: FileSystem.FileSystem["open"] = Effect.fnUntraced(function* (filename, options) {
  const flag = options?.flag ?? "r";
  if (options?.noFollow && (process.platform === "win32" || !constants.O_NOFOLLOW))
    return yield* badArgument({
      module: "FileSystem",
      method: "open",
      description: "noFollow is unavailable",
    });
  const native = yield* Effect.acquireRelease(
    io("open", () =>
      open(filename, options?.noFollow ? flags[flag] | constants.O_NOFOLLOW : flag, options?.mode),
    ),
    (file) => io("close", () => file.close()).pipe(Effect.orDie),
  );
  let cursor = 0n;
  const append = flag.startsWith("a");
  const read: FileSystem.File["read"] = Effect.fnUntraced(function* (buffer, readOptions) {
    const position = readOptions?.position ?? cursor;
    const offset = yield* positionNumber(position);
    const result = yield* io("read", () => native.read(buffer, 0, buffer.length, offset));
    if (readOptions?.position === undefined) cursor = position + BigInt(result.bytesRead);
    return result.bytesRead;
  });
  const write: FileSystem.File["write"] = Effect.fnUntraced(function* (buffer) {
    const offset = yield* positionNumber(cursor);
    const result = yield* io("write", () =>
      native.write(buffer, 0, buffer.length, append ? null : offset),
    );
    if (!append) cursor += BigInt(result.bytesWritten);
    return result.bytesWritten;
  });
  const file: FileSystem.File = {
    [FileSystem.FileTypeId]: FileSystem.FileTypeId,
    stat: io("stat", () => native.stat({ bigint: true })).pipe(Effect.map(info)),
    sync: io("sync", () => native.sync()),
    seek: (offset, from) =>
      Effect.gen(function* () {
        const position = from === "start" ? offset : cursor + offset;
        yield* positionNumber(position);
        cursor = position;
        return position;
      }),
    read,
    readAlloc: (size, readOptions) =>
      Effect.gen(function* () {
        if (!Number.isSafeInteger(size) || size < 0)
          return yield* badArgument({ module: "FileSystem", method: "readAlloc" });
        const buffer = new Uint8Array(size);
        const count = yield* read(buffer, readOptions);
        return count === 0 ? Option.none() : Option.some(buffer.subarray(0, count));
      }),
    truncate: (length = 0) =>
      Effect.gen(function* () {
        if (!Number.isSafeInteger(length) || length < 0)
          return yield* badArgument({ module: "FileSystem", method: "truncate" });
        yield* io("truncate", () => native.truncate(length));
        if (cursor > BigInt(length)) cursor = BigInt(length);
      }),
    write,
    writeAll: (bytes) =>
      Effect.gen(function* () {
        let offset = 0;
        while (offset < bytes.length) {
          const count = yield* write(bytes.subarray(offset));
          if (count === 0)
            return yield* systemError({
              module: "FileSystem",
              method: "writeAll",
              _tag: "WriteZero",
            });
          offset += count;
        }
      }),
  };
  return file;
});

export const layer = Layer.effect(
  FileSystem.FileSystem,
  Effect.map(FileSystem.FileSystem, (fs) => FileSystem.make({ ...fs, open: openFile })),
).pipe(Layer.provide(BunFileSystem.layer));
