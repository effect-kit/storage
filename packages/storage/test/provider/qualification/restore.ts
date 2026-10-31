import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Effect, Redacted } from "effect";

import { Storage } from "@effect-kit/storage";
import { StorageCompression } from "@effect-kit/storage/compression";
import { StorageEncryption, StorageEncryptionCodec } from "@effect-kit/storage/encryption";
import { AgeEncryption } from "@effect-kit/storage/encryption/age";
import { FilesystemStorage } from "@effect-kit/storage/filesystem";
import { layer as platform } from "@effect-kit/storage/node";
import { generateX25519Identity, identityToRecipient } from "age-encryption";

const [phase, directory] = process.argv.slice(2);
assert(directory && (phase === "seed" || phase === "restore"));
const identityPath = join(directory, "identity.json");
const identity: string =
  phase === "seed"
    ? await generateX25519Identity()
    : JSON.parse(await readFile(identityPath, "utf8"));
if (phase === "seed") await writeFile(identityPath, JSON.stringify(identity), { mode: 0o600 });
const recipient = await identityToRecipient(identity);
const limits = {
  maxDecodedBytes: 2 * 1024 ** 2,
  maxStoredBytes: 4 * 1024 ** 2,
  maxExpansionRatio: 10000,
};
const payload = new Uint8Array(1024 * 1024 + 17).map((_, index) => index % 251);

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const backing = yield* FilesystemStorage.make({
        root: join(directory, phase === "seed" ? "source" : "backup"),
        storeId: "restore-fixture",
      });
      const codec = yield* AgeEncryption.make({
        recipients: [recipient],
        identities: Effect.succeed([Redacted.make(identity)]),
      });
      const encrypted = yield* StorageEncryption.make(limits).pipe(
        Effect.provideService(StorageEncryptionCodec, codec),
        Effect.provideService(Storage, backing),
      );
      const storage = yield* StorageCompression.make({ ...limits, format: "gzip" }).pipe(
        Effect.provideService(Storage, encrypted),
      );
      if (phase === "seed") {
        yield* storage.put({ key: "backup/object", body: payload });
        return;
      }
      const restored = yield* Storage.getBytes({
        key: "backup/object",
        maxBytes: payload.length,
      }).pipe(Effect.provideService(Storage, storage));
      assert.deepEqual(restored, payload);
      assert.equal((yield* storage.head({ key: "backup/object" })).size, payload.length);
    }),
  ).pipe(Effect.provide(platform)),
);
process.stdout.write(`${phase}: passed\n`);
