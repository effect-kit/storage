import { assert, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, Path, Result, Stream } from "effect";
import { Digest } from "effect/crypto";

import { FilesystemStorage } from "../../src/filesystem.js";
import * as NodeStorage from "../../src/node.js";
import { Storage } from "../../src/storage.js";
import { runStorageConformance } from "../../src/testing.js";
import { TransferDriver, Transfers, UploadSource } from "../../src/transfers.js";

const fixture = Effect.fnUntraced(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "storage-filesystem-" });
  const options = { root, storeId: "filesystem-tests" };
  const storage = yield* FilesystemStorage.make(options);
  return { fs, path, root, options, storage };
});
const read = (storage: Storage["Service"], key: string) =>
  Storage.getBytes({ key, maxBytes: 1024 }).pipe(Effect.provideService(Storage, storage));

it.effect("filesystem passes the portable contract with its advertised capabilities", () =>
  runStorageConformance(() => Effect.map(fixture(), (test) => test.storage)).pipe(
    Effect.tap((results) =>
      Effect.sync(() => {
        assert.strictEqual(results.filter((result) => result.status === "passed").length, 12);
        assert.strictEqual(results.filter((result) => result.status === "skipped").length, 1);
      }),
    ),
    Effect.provide(NodeStorage.layer),
  ),
);

it.effect("opaque hostile-looking keys remain distinct objects inside the root", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const keys = [
      "../escape",
      "/absolute",
      "a//b",
      "a/b",
      "a\\b",
      "C:\\file",
      "\\\\server\\share",
      "%2F",
      "CON",
      "a.",
      "Name",
      "name",
      "é",
      "é",
      "staging/file",
      "format.json",
    ];
    for (const [index, key] of keys.entries())
      yield* test.storage.put({ key, body: new Uint8Array([index]) });
    for (const [index, key] of keys.entries())
      assert.deepEqual(yield* read(test.storage, key), new Uint8Array([index]));
    assert.strictEqual(
      (yield* test.fs.readDirectory(test.path.join(test.root, "objects"))).length,
      keys.length,
    );
    assert.deepEqual((yield* test.fs.readDirectory(test.root)).toSorted(), [
      "format.json",
      "objects",
      "staging",
    ]);
  }).pipe(Effect.provide(NodeStorage.layer)),
);

it.effect("independent writers race complete-file create-only publication", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const other = yield* FilesystemStorage.make(test.options);
    const results = yield* Effect.all(
      [test.storage, other].map((storage, index) =>
        storage
          .put({
            key: "race",
            body: new Uint8Array(65537).fill(index + 1),
            condition: { _tag: "CreateOnly" },
          })
          .pipe(Effect.result),
      ),
      { concurrency: 2 },
    );
    assert.strictEqual(results.filter(Result.isSuccess).length, 1);
    const loser = results.find(Result.isFailure);
    assert.strictEqual(loser?.failure.reason._tag, "PreconditionFailed");
    assert.strictEqual(loser?.failure.mutationOutcome, "rejected");
    const bytes = yield* Storage.getBytes({ key: "race", maxBytes: 65537 }).pipe(
      Effect.provideService(Storage, test.storage),
    );
    assert.strictEqual(bytes.length, 65537);
    assert.isTrue(bytes.every((byte) => byte === bytes[0]));
  }).pipe(Effect.provide(NodeStorage.layer)),
);

it.effect("an opened body and metadata retain the same generation across replacement", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* fixture();
      const first = yield* test.storage.put({
        key: "same",
        body: new Uint8Array([1, 1]),
        contentType: "first",
      });
      const response = yield* test.storage.get({ key: "same" });
      yield* test.storage.put({ key: "same", body: new Uint8Array([2]), contentType: "second" });
      assert.strictEqual(response.metadata.etag, first.etag);
      assert.strictEqual(response.metadata.contentType, "first");
      if (response._tag !== "Object") return yield* Effect.die("missing body");
      const chunks = yield* Stream.runCollect(response.body);
      assert.deepEqual(
        Array.from(chunks).flatMap((chunk) => Array.from(chunk)),
        [1, 1],
      );
      assert.deepEqual(yield* read(test.storage, "same"), new Uint8Array([2]));
    }),
  ).pipe(Effect.provide(NodeStorage.layer)),
);

it.effect("source failure and false length leave the previous object and no staging files", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    yield* test.storage.put({ key: "safe", body: new Uint8Array([1]) });
    const sourceError = { _tag: "ApplicationSourceFailure" } as const;
    const failed = yield* test.storage
      .put({
        key: "safe",
        body: Stream.concat(Stream.succeed(new Uint8Array([2])), Stream.fail(sourceError)),
      })
      .pipe(Effect.result);
    assert.isTrue(Result.isFailure(failed));
    if (Result.isFailure(failed)) assert.strictEqual(failed.failure, sourceError);
    const length = yield* test.storage
      .put({ key: "safe", body: new Uint8Array([2]), contentLength: 2 })
      .pipe(Effect.result);
    assert.isTrue(Result.isFailure(length));
    if (Result.isFailure(length)) assert.strictEqual(length.failure.reason._tag, "LengthMismatch");
    assert.deepEqual(yield* read(test.storage, "safe"), new Uint8Array([1]));
    assert.deepEqual(yield* test.fs.readDirectory(test.path.join(test.root, "staging")), []);
  }).pipe(Effect.provide(NodeStorage.layer)),
);

it.effect("unsupported match-current writes reject before consuming their source", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    let consumed = false;
    const result = yield* test.storage
      .put({
        key: "safe",
        condition: { _tag: "IfMatch", etag: "*" },
        body: Stream.fromEffect(
          Effect.sync(() => {
            consumed = true;
            return new Uint8Array();
          }),
        ),
      })
      .pipe(Effect.result);
    assert.isFalse(consumed);
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result)) assert.strictEqual(result.failure.reason._tag, "Unsupported");
  }).pipe(Effect.provide(NodeStorage.layer)),
);

it.effect("symlink roots, internal directories and object files fail closed", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const linked = test.path.join(test.root, "linked");
    yield* test.fs.symlink(test.path.join(test.root, "objects"), linked);
    assert.isTrue(
      Result.isFailure(
        yield* FilesystemStorage.make({ root: linked, storeId: "other" }).pipe(Effect.result),
      ),
    );
    yield* test.fs.remove(linked);
    const identities = yield* Digest.Digest;
    const name = yield* identities.digestHex(
      Digest.sha256,
      new TextEncoder().encode("linked-object"),
    );
    const object = test.path.join(test.root, "objects", `${name}.object`);
    yield* test.fs.symlink(test.path.join(test.root, "format.json"), object);
    assert.isTrue(
      Result.isFailure(yield* test.storage.head({ key: "linked-object" }).pipe(Effect.result)),
    );
    assert.isTrue(
      Result.isFailure(
        yield* test.storage
          .put({ key: "linked-object", body: new Uint8Array() })
          .pipe(Effect.result),
      ),
    );
    yield* test.fs.rename(test.path.join(test.root, "objects"), test.path.join(test.root, "saved"));
    yield* test.fs.symlink(
      test.path.join(test.root, "saved"),
      test.path.join(test.root, "objects"),
    );
    assert.isTrue(Result.isFailure(yield* test.storage.listPage().pipe(Effect.result)));
  }).pipe(Effect.provide(NodeStorage.layer)),
);

it.effect("root identity, corrupt envelopes and cursor isolation fail explicitly", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    assert.isTrue(
      Result.isFailure(
        yield* FilesystemStorage.make({ ...test.options, storeId: "wrong" }).pipe(Effect.result),
      ),
    );
    for (const key of ["a", "b"]) yield* test.storage.put({ key, body: new Uint8Array([1]) });
    const page = yield* test.storage.listPage({ limit: 1 });
    const other = yield* fixture();
    assert.isTrue(
      Result.isFailure(
        yield* other.storage.listPage({ cursor: page.cursor, limit: 1 }).pipe(Effect.result),
      ),
    );
    const names = yield* test.fs.readDirectory(test.path.join(test.root, "objects"));
    yield* test.fs.writeFile(
      test.path.join(test.root, "objects", names[0] ?? "missing"),
      new Uint8Array([1]),
    );
    assert.isTrue(Result.isFailure(yield* test.storage.listPage().pipe(Effect.result)));
  }).pipe(Effect.provide(NodeStorage.layer)),
);

it.effect("provider layers supply a matching driver for managed upload and verified download", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const test = yield* fixture();
      const context = yield* Layer.build(
        Transfers.layer({ maxBytes: 1024 }).pipe(
          Layer.provideMerge(FilesystemStorage.layer(test.options)),
        ),
      );
      const driver = yield* TransferDriver.pipe(Effect.provide(context));
      const storage = yield* Storage.pipe(Effect.provide(context));
      assert.strictEqual(driver.storage, storage);
      const transfers = yield* Transfers.pipe(Effect.provide(context));
      const receipt = yield* transfers.upload({
        key: "managed",
        source: UploadSource.bytes(new Uint8Array([1, 2, 3])),
      });
      const chunks: number[] = [];
      yield* transfers.download({
        ref: receipt.metadata.ref,
        expectedChecksum: receipt.checksum,
        target: {
          write: (chunk) =>
            Effect.sync(() => {
              chunks.push(...chunk);
            }),
        },
      });
      assert.deepEqual(chunks, [1, 2, 3]);
    }),
  ).pipe(Effect.provide(NodeStorage.layer)),
);
