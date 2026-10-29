import { assert, it } from "@effect/vitest";
import { ByteSize, Effect, FileSystem, Option, Path, Result, Stream } from "effect";

import { Transfers, UploadSource } from "../../src/transfers.js";
import { sha256 } from "../../src/transfers/node.js";
import { fixture, policy } from "./fixture.js";

function files() {
  const contents = new Map<string, Uint8Array>([["source", new Uint8Array([1, 2, 3])]]);
  let openHandles = 0;
  let renames = 0;
  const info = (path: string): FileSystem.File.Info => ({
    type: "File",
    size: ByteSize.bytes(contents.get(path)?.length ?? 0),
    dev: 1n,
    ino: Option.some(1n),
    mtime: Option.some(new Date(0)),
    atime: Option.none(),
    birthtime: Option.none(),
    mode: 0o600,
    nlink: Option.none(),
    uid: Option.none(),
    gid: Option.none(),
    rdev: Option.none(),
    blksize: Option.none(),
    blocks: Option.none(),
  });
  const fs = FileSystem.makeNoop({
    stat: (path) => Effect.sync(() => info(path)),
    makeTempFileScoped: () =>
      Effect.acquireRelease(
        Effect.sync(() => {
          contents.set("temporary", new Uint8Array());
          return "temporary";
        }),
        () =>
          Effect.sync(() => {
            contents.delete("temporary");
          }),
      ),
    rename: (from, to) =>
      Effect.sync(() => {
        assert.strictEqual(openHandles, 0);
        renames++;
        contents.set(to, contents.get(from) ?? new Uint8Array());
        contents.delete(from);
      }),
    open: (path, options) =>
      Effect.acquireRelease(
        Effect.sync((): FileSystem.File => {
          openHandles++;
          if (options?.flag === "w") contents.set(path, new Uint8Array());
          let offset = 0;
          return {
            [FileSystem.FileTypeId]: FileSystem.FileTypeId,
            stat: Effect.sync(() => info(path)),
            sync: Effect.void,
            seek: (count, from) =>
              Effect.sync(() => {
                offset = (from === "start" ? 0 : offset) + Number(count);
                return BigInt(offset);
              }),
            read: () => Effect.die("fixture uses readAlloc"),
            readAlloc: (size) =>
              Effect.sync(() => {
                const bytes = contents.get(path) ?? new Uint8Array();
                if (offset === bytes.length) return Option.none();
                const chunk = bytes.slice(offset, offset + size);
                offset += chunk.length;
                return Option.some(chunk);
              }),
            write: () => Effect.die("fixture uses writeAll"),
            writeAll: (chunk) =>
              Effect.sync(() => {
                contents.set(path, Uint8Array.from([...(contents.get(path) ?? []), ...chunk]));
              }),
            truncate: () => Effect.die("fixture does not truncate"),
          };
        }),
        () =>
          Effect.sync(() => {
            openHandles--;
          }),
      ),
  });
  return { fs, contents, stats: () => ({ openHandles, renames }) };
}

it.effect("file sources reject same-size edits with unchanged identity before replay", () =>
  Effect.gen(function* () {
    const test = files();
    const source = yield* UploadSource.file("source", sha256).pipe(
      Effect.provideService(FileSystem.FileSystem, test.fs),
    );
    assert.deepEqual(yield* Stream.runCollect(source.open()), [new Uint8Array([1, 2, 3])]);
    test.contents.set("source", new Uint8Array([9, 2, 3]));
    const changed = yield* Stream.runDrain(source.open()).pipe(Effect.result);
    assert.isTrue(Result.isFailure(changed) && changed.failure.reason._tag === "InvalidInput");
    assert.strictEqual(test.stats().openHandles, 0);
  }),
);

it.effect("file downloads publish only verified bytes after closing the staging handle", () =>
  Effect.gen(function* () {
    const test = files();
    const provider = yield* fixture();
    yield* provider.endpoint.storage.put({ key: "object", body: new Uint8Array([7, 8]) });
    const transfers = yield* Transfers.make(provider.endpoint, policy);
    const provide = <A, E>(
      effect: Effect.Effect<A, E, Transfers | FileSystem.FileSystem | Path.Path>,
    ) =>
      effect.pipe(
        Effect.provideService(Transfers, transfers),
        Effect.provideService(FileSystem.FileSystem, test.fs),
        Effect.provide(Path.layer),
      );
    yield* provide(Transfers.downloadFile({ key: "object", path: "destination" }));
    assert.deepEqual(test.contents.get("destination"), new Uint8Array([7, 8]));
    const mismatch = yield* provide(
      Transfers.downloadFile({
        key: "object",
        path: "destination",
        expectedChecksum: {
          algorithm: "sha256",
          encoding: "hex",
          scope: "full-object",
          value: "00",
        },
      }),
    ).pipe(Effect.result);
    assert.isTrue(Result.isFailure(mismatch));
    assert.strictEqual(test.stats().renames, 1);
    assert.isFalse(test.contents.has("temporary"));
    assert.deepEqual(test.contents.get("destination"), new Uint8Array([7, 8]));
  }),
);
