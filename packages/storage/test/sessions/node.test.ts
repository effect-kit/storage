import { randomBytes } from "node:crypto";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result } from "effect";
import { KeyValueStore } from "effect/persistence";

import {
  layer as platformLayer,
  aes256Gcm,
  atomicFileStore,
  fileDownloadTarget,
} from "../../src/node.js";
import { UploadSessionStore } from "../../src/sessions.js";
import { UploadSource } from "../../src/transfers.js";
import { setup, makeSessions } from "./fixture.js";

it.live("file backing rejects enumeration and prefix removal without losing checkpoints", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => mkdtemp(join(tmpdir(), "storage-session-inventory-"))),
        (path) => Effect.promise(() => rm(path, { recursive: true, force: true })),
      );
      const backing = yield* atomicFileStore(directory);
      yield* backing.set("test:session", "protected-checkpoint");

      const inventory = yield* backing.keys.pipe(Effect.result);
      assert.isTrue(Result.isFailure(inventory));
      if (Result.isFailure(inventory)) assert.strictEqual(inventory.failure.method, "keys");

      const removal = yield* backing.clearPrefix("test:").pipe(Effect.result);
      assert.isTrue(Result.isFailure(removal));
      assert.strictEqual(yield* backing.get("test:session"), "protected-checkpoint");
    }),
  ).pipe(Effect.provide(platformLayer)),
);

it.live("real disk stores survive reconstruction and download publication is idempotent", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => mkdtemp(join(tmpdir(), "storage-session-files-"))),
        (path) => Effect.promise(() => rm(path, { recursive: true, force: true })),
      );
      const kvs = yield* atomicFileStore(directory);
      const test = yield* setup().pipe(Effect.provideService(KeyValueStore.KeyValueStore, kvs));
      const first = yield* test.make();
      const source = UploadSource.bytes(new Uint8Array([1, 2, 3]));
      const id = yield* first.create({ key: "disk-checkpoint", source });
      const secondKvs = yield* atomicFileStore(directory);
      const secondStore = yield* UploadSessionStore.make({
        namespace: "test",
        ownership: "exclusive",
        protection: test.protection,
      }).pipe(Effect.provideService(KeyValueStore.KeyValueStore, secondKvs));
      const second = yield* makeSessions(test.endpoint, secondStore, {
        configurationId: "test-config",
      });
      const metadata = yield* second.resume(id, source);
      const path = join(directory, "destination");
      const target = yield* fileDownloadTarget({ id: "download", path });
      yield* second.createDownload({ id: "download", ref: metadata.ref });
      yield* second.resumeDownload("download", target);
      assert.deepEqual(
        new Uint8Array(yield* Effect.promise(() => readFile(path))),
        new Uint8Array([1, 2, 3]),
      );
      const checkpoint = yield* secondStore.read("download");
      if (checkpoint?.kind === "download" && checkpoint.checksum)
        yield* target.publish(checkpoint.checksum, checkpoint.length);
      assert.isFalse(
        (yield* Effect.promise(() => readdir(directory))).some((name) =>
          name.startsWith(".pending-"),
        ),
      );
    }),
  ).pipe(Effect.provide(platformLayer)),
);

it.effect("AES-GCM authenticates namespace/session identity and rejects changed keys", () =>
  Effect.gen(function* () {
    const key = Redacted.make(randomBytes(32));
    const protection = aes256Gcm(key);
    const ciphertext = yield* protection.seal("private-upload-handle", "tenant:session");
    assert.strictEqual(
      yield* protection.open(ciphertext, "tenant:session"),
      "private-upload-handle",
    );
    assert.isFalse(JSON.stringify(ciphertext).includes("private-upload-handle"));
    assert.isTrue(
      Result.isFailure(yield* protection.open(ciphertext, "other:session").pipe(Effect.result)),
    );
    assert.isTrue(
      Result.isFailure(
        yield* aes256Gcm(Redacted.make(randomBytes(32)))
          .open(ciphertext, "tenant:session")
          .pipe(Effect.result),
      ),
    );
  }),
);
