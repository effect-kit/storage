import { ByteSize, Effect, FileSystem, Path, Schema, Stream } from "effect";
import { Digest } from "effect/crypto";

import type { StorageError } from "../errors.js";
import type { Checksum } from "../object.js";
import { SessionId } from "../sessions/checkpoint.js";
import type { RestartableDownloadTarget } from "../sessions/download.js";
import { sessionError } from "../sessions/store.js";
import { TransferHash } from "../transfers/dependencies.js";
import { sameChecksum } from "../transfers/reader.js";
import { syncDirectory } from "./filesystem.js";

// Platform IO may be non-cancellable: join issued writes before releasing ownership.
const mutate = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.uninterruptible,
    Effect.mapError(() => sessionError("SessionFileIO")),
  );

export const fileDownloadTarget = Effect.fnUntraced(function* (options: {
  readonly id: string;
  readonly path: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const digest = yield* TransferHash;
  const identities = yield* Digest.Digest;
  const input = yield* Schema.decodeUnknownEffect(
    Schema.Struct({ id: SessionId, path: Schema.NonEmptyString }),
  )(options).pipe(Effect.mapError(() => sessionError("InvalidDownloadTarget")));
  const destination = path.resolve(input.path);
  const partial = `${destination}.${input.id}.partial`;
  const identity = yield* identities.digestHex(
    Digest.sha256,
    new TextEncoder().encode(`${input.id}\0${destination}\0${partial}`),
  );

  const verify = Effect.fnUntraced(function* (
    filename: string,
    expected: Checksum,
    length: number,
  ) {
    const info = yield* fs.stat(filename);
    if (ByteSize.toBigInt(info.size) !== BigInt(length))
      return yield* sessionError("PartialLengthMismatch");
    const hash = digest.create();
    yield* Stream.runForEach(fs.stream(filename, { chunkSize: 65536 }), (bytes) =>
      Effect.sync(() => hash.update(bytes)),
    );
    if (!sameChecksum(hash.finish(), expected))
      return yield* sessionError("PartialIntegrityMismatch");
  });
  const target: RestartableDownloadTarget<StorageError> = {
    id: input.id,
    identity,
    prefix: (length) =>
      length === 0
        ? Stream.empty
        : fs
            .stream(partial, { bytesToRead: length, chunkSize: 65536 })
            .pipe(Stream.mapError(() => sessionError("PartialRead"))),
    truncate: (length) =>
      mutate(
        Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* fs.open(partial, {
              flag: length === 0 ? "a+" : "r+",
              mode: 0o600,
            });
            if (ByteSize.toBigInt((yield* handle.stat).size) < BigInt(length))
              return yield* sessionError("PartialTooShort");
            yield* handle.truncate(length);
            yield* handle.sync;
          }),
        ).pipe(Effect.andThen(syncDirectory(fs, path.dirname(destination)))),
      ),
    write: (offset, bytes) =>
      mutate(
        Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* fs.open(partial, { flag: "r+" });
            yield* handle.seek(BigInt(offset), "start");
            yield* handle.writeAll(bytes);
          }),
        ),
      ),
    sync: mutate(
      Effect.scoped(Effect.flatMap(fs.open(partial, { flag: "r+" }), (handle) => handle.sync)).pipe(
        Effect.andThen(syncDirectory(fs, path.dirname(destination))),
      ),
    ),
    publish: (checksum, length) =>
      mutate(
        Effect.gen(function* () {
          if (!(yield* fs.exists(partial))) {
            yield* verify(destination, checksum, length);
            return;
          }
          yield* verify(partial, checksum, length);
          yield* fs.rename(partial, destination);
          yield* syncDirectory(fs, path.dirname(destination));
        }),
      ),
    discard: () =>
      mutate(
        fs
          .remove(partial, { force: true })
          .pipe(Effect.andThen(syncDirectory(fs, path.dirname(destination)))),
      ),
  };
  return target;
});
