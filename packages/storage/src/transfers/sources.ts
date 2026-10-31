import { ByteSize, Cause, Effect, FileSystem, Option, Stream } from "effect";
import type { PlatformError } from "effect/PlatformError";

import type { StorageError } from "../core/errors.js";
import { storageError } from "../core/errors.js";
import { TransferHash } from "./dependencies.js";

export interface UploadSource<E = never, R = never> {
  readonly length?: number | undefined;
  readonly replayable: boolean;
  readonly workspaceBytes?: number | undefined;
  readonly open: () => Stream.Stream<Uint8Array, E | StorageError, R>;
}

const sourceError = (field: string) =>
  storageError("source", "transfer", { _tag: "InvalidInput", field }, "not-started");

/** Copies the caller's bytes once. This immutable snapshot is outside the engine's workspace. */
export function bytes(input: Uint8Array): UploadSource {
  const snapshot = Uint8Array.from(input);
  return replayable(
    () =>
      Stream.fromIterable(
        (function* () {
          for (let start = 0; start < snapshot.length; start += 65536)
            yield snapshot.slice(start, start + 65536);
        })(),
        { chunkSize: 1 },
      ),
    snapshot.length,
  );
}

export function stream<E, R>(
  body: Stream.Stream<Uint8Array, E, R>,
  length?: number,
): UploadSource<E, R> {
  let consumed = false;
  return {
    length,
    replayable: false,
    open: () =>
      Stream.unwrap(
        Effect.suspend(() => {
          if (consumed)
            return Effect.fail(
              storageError("source", "transfer", { _tag: "BodyConsumed" }, "not-started"),
            );
          consumed = true;
          return Effect.succeed(body);
        }),
      ),
  };
}

/** The factory promises the same representation on every opening. */
export function replayable<E, R>(
  open: () => Stream.Stream<Uint8Array, E, R>,
  length?: number,
): UploadSource<E, R> {
  return { length, replayable: true, open: () => Stream.suspend(open) };
}

const identity = (info: FileSystem.File.Info) =>
  `${String(info.dev)}:${Option.getOrUndefined(info.ino)}:${String(ByteSize.toBigInt(info.size))}:${Option.getOrUndefined(info.mtime)?.getTime()}`;

/** Captures FileSystem once. Every opening checks identity and a full bounded-memory fingerprint. */
export const file = Effect.fnUntraced(function* (
  path: string,
): Effect.fn.Return<
  UploadSource<PlatformError | StorageError>,
  PlatformError | StorageError,
  FileSystem.FileSystem | TransferHash
> {
  const fs = yield* FileSystem.FileSystem;
  const digest = yield* TransferHash;
  const original = yield* fs.stat(path);
  const length = Number(ByteSize.toBigInt(original.size));
  if (original.type !== "File" || !Number.isSafeInteger(length)) return yield* sourceError("file");

  const fingerprint = Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* fs.open(path);
      const state = digest.create();
      while (true) {
        const chunk = yield* handle.readAlloc(65536);
        if (Option.isNone(chunk)) break;
        state.update(chunk.value);
      }
      return state.finish().value;
    }),
  );
  const expected = yield* fingerprint;

  const source = replayable(
    () =>
      Stream.unwrap(
        Effect.gen(function* () {
          const current = yield* fs.stat(path);
          if (identity(current) !== identity(original) || (yield* fingerprint) !== expected)
            return yield* sourceError("file.changed");

          const state = digest.create();
          const handle = yield* fs.open(path);
          const body = Stream.fromPull(
            Effect.succeed(
              Effect.gen(function* () {
                const chunk = yield* handle.readAlloc(65536);
                if (Option.isNone(chunk)) return yield* Cause.done();
                return [chunk.value] as const;
              }),
            ),
          );
          return body.pipe(
            Stream.tap((chunk) => Effect.sync(() => state.update(chunk))),
            Stream.concat(
              Stream.drain(
                Stream.fromEffect(
                  Effect.gen(function* () {
                    const after = yield* fs.stat(path);
                    if (identity(after) !== identity(original) || state.finish().value !== expected)
                      return yield* sourceError("file.changed");
                  }),
                ),
              ),
            ),
          );
        }),
      ),
    length,
  );
  return { ...source, workspaceBytes: 65536 + digest.workspaceBytes };
});
