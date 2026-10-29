import { Effect, FileSystem, Result, Stream } from "effect";

import * as BunPlatform from "../../../dist/bun.js";
import { FilesystemStorage } from "../../../dist/filesystem.js";
import { Storage } from "../../../dist/index.js";
import * as NodePlatform from "../../../dist/node.js";
import { runStorageConformance } from "../../../dist/testing.js";

const [runtime, root, command, boundary] = process.argv.slice(2);
const platform = runtime === "bun" ? BunPlatform : NodePlatform;
const stop = (name) =>
  name === boundary
    ? Effect.sync(() => process.stdout.write(`${JSON.stringify({ boundary: name })}\n`)).pipe(
        Effect.andThen(Effect.never),
      )
    : Effect.void;
const program = Effect.scoped(
  Effect.gen(function* () {
    const actual = yield* FileSystem.FileSystem;
    const wrapped = FileSystem.make({
      ...actual,
      open: (filename, options) =>
        actual.open(filename, options).pipe(
          Effect.map((file) => ({
            [FileSystem.FileTypeId]: FileSystem.FileTypeId,
            stat: file.stat,
            seek: (...args) => file.seek(...args),
            read: (...args) => file.read(...args),
            readAlloc: (...args) => file.readAlloc(...args),
            truncate: (...args) => file.truncate(...args),
            write: (...args) => file.write(...args),
            writeAll: (bytes) =>
              file
                .writeAll(bytes)
                .pipe(
                  Effect.andThen(
                    filename.includes("/staging/") ? stop("body-written") : Effect.void,
                  ),
                ),
            sync: filename.endsWith("/objects")
              ? stop("directory-sync-before").pipe(
                  Effect.andThen(file.sync),
                  Effect.andThen(stop("directory-sync-after")),
                )
              : file.sync.pipe(
                  Effect.andThen(
                    filename.includes("/staging/") ? stop("staged-synced") : Effect.void,
                  ),
                ),
          })),
        ),
      rename: (from, to) =>
        stop("rename-before").pipe(
          Effect.andThen(actual.rename(from, to)),
          Effect.andThen(stop("rename-after")),
        ),
      link: (from, to) =>
        stop("link-before").pipe(
          Effect.andThen(actual.link(from, to)),
          Effect.andThen(stop("link-after")),
        ),
    });
    const storage = yield* FilesystemStorage.make({ root, storeId: "filesystem-acceptance" }).pipe(
      Effect.provideService(FileSystem.FileSystem, command === "crash" ? wrapped : actual),
    );
    if (command === "seed") {
      yield* storage.put({ key: "object", body: new Uint8Array([1]), contentType: "old" });
      return { seeded: true };
    }
    if (command === "inspect") {
      const metadata = yield* storage.head({ key: "object" });
      const bytes = yield* Storage.getBytes({ key: "object", maxBytes: 262144 }).pipe(
        Effect.provideService(Storage, storage),
      );
      const removed = yield* storage.reclaimStaging({ ownership: "exclusive" });
      const created = yield* Storage.getBytes({ key: "created", maxBytes: 262144 }).pipe(
        Effect.provideService(Storage, storage),
        Effect.map((createdBytes) => ({
          size: createdBytes.length,
          uniform: createdBytes.every((byte) => byte === 2),
        })),
        Effect.catchReason("StorageError", "NotFound", () => Effect.undefined),
      );
      return {
        created,
        size: bytes.length,
        first: bytes[0],
        uniform: bytes.every((byte) => byte === bytes[0]),
        contentType: metadata.contentType,
        removed,
      };
    }
    if (command === "race") {
      const result = yield* storage
        .put({
          key: "race",
          body: new Uint8Array(131072).fill(Number(boundary)),
          condition: { _tag: "CreateOnly" },
        })
        .pipe(Effect.result);
      return Result.isSuccess(result)
        ? { result: "winner" }
        : { result: result.failure.reason._tag };
    }
    if (command === "conformance") {
      const results = yield* runStorageConformance(() =>
        Effect.gen(function* () {
          const directory = yield* actual.makeTempDirectoryScoped({
            directory: root,
            prefix: "case-",
          });
          return yield* FilesystemStorage.make({ root: directory, storeId: "conformance" });
        }),
      );
      // Exercise a large streaming put without collecting the source in the adapter.
      const block = new Uint8Array(65536).fill(7);
      const metadata = yield* storage.put({
        key: "streamed",
        body: Stream.fromIterable(Array.from({ length: 512 }, () => block)),
        contentLength: 32 * 1024 * 1024,
      });
      return {
        results,
        streamedBytes: metadata.size,
        runtime: runtime === "bun" ? Bun.version : process.version,
      };
    }
    yield* storage.put({
      key: "object",
      body: Stream.make(new Uint8Array(65536).fill(2), new Uint8Array(65536).fill(2)),
      contentType: "new",
      ...(boundary?.startsWith("link-")
        ? { key: "created", condition: { _tag: "CreateOnly" } }
        : {}),
    });
    return { unexpected: "crash boundary was not reached" };
  }),
).pipe(Effect.provide(platform.layer));

process.stdout.write(`${JSON.stringify(await Effect.runPromise(program))}\n`);
