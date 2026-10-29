import { randomBytes } from "node:crypto";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result } from "effect";
import { KeyValueStore } from "effect/persistence";

import { UploadSessions, UploadSessionStore } from "../../src/sessions.js";
import { aes256Gcm, atomicFileStore, fileDownloadTarget } from "../../src/sessions/node.js";
import { UploadSource } from "../../src/transfers.js";
import { policy } from "../transfers/fixture.js";
import { handles, setup } from "./fixture.js";

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
      const second = yield* UploadSessions.make(test.endpoint, policy, {
        configurationId: "test-config",
        handles,
      }).pipe(Effect.provideService(UploadSessionStore, secondStore));
      const metadata = yield* second.resume(id, source);
      const path = join(directory, "destination");
      const target = fileDownloadTarget({ id: "download", path });
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
  ),
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
