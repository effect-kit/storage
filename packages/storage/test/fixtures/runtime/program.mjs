import { Context, Effect, FileSystem, Layer, Path, Redacted, Result, Stream } from "effect";
import { SecureRandom } from "effect/crypto";
import { KeyValueStore } from "effect/persistence";

import { UploadSessions, UploadSessionStore } from "@effect-kit/storage/sessions";
import { MemoryStorage } from "@effect-kit/storage/testing";
import { TransferHash, Transfers, UploadSource } from "@effect-kit/storage/transfers";

const check = (condition, message) => {
  if (!condition) throw new Error(message);
};
const bytes = new TextEncoder().encode("abc");
const expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

export const runRuntime = (platform, name, files) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const checks = [];
        const hash = yield* TransferHash;
        const state = hash.create();
        state.update(bytes.subarray(0, 1));
        const prefix = state.snapshot();
        check(
          prefix.value === "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
          "prefix digest",
        );
        state.update(bytes.subarray(1));
        check(state.finish().value === expected, "incremental SHA-256");
        checks.push("incremental SHA-256 and non-consuming snapshot");

        const random = yield* SecureRandom.SecureRandom;
        const secret = Redacted.make(yield* random.randomBytes(32));
        const selected = platform.aes256Gcm(secret);
        const protection = Effect.isEffect(selected) ? yield* selected : selected;
        const sealed = yield* protection.seal("checkpoint", "runtime:session");
        check(
          (yield* protection.open(sealed, "runtime:session")) === "checkpoint",
          "AEAD roundtrip",
        );
        check(
          Result.isFailure(yield* protection.open(sealed, "wrong:session").pipe(Effect.result)),
          "AEAD identity binding",
        );
        checks.push("AES-GCM and associated-data rejection");

        const context = yield* Layer.build(
          Transfers.layer({ maxBytes: 1024 * 1024 }).pipe(
            Layer.provideMerge(MemoryStorage.layer()),
          ),
        );
        const transfers = Context.get(context, Transfers);
        const receipt = yield* transfers.upload({
          key: "runtime",
          source: UploadSource.bytes(bytes),
        });
        check(receipt.checksum.value === expected, "managed checksum");
        const downloaded = [];
        yield* transfers.download({
          ref: receipt.metadata.ref,
          expectedChecksum: receipt.checksum,
          target: {
            write: (chunk) => Effect.sync(() => downloaded.push(...chunk)).pipe(Effect.asVoid),
          },
        });
        check(downloaded.join(",") === [...bytes].join(","), "managed bytes");
        checks.push("provider-layer managed upload/download");

        let directory;
        let store;
        if (files) {
          const fs = yield* FileSystem.FileSystem;
          directory = yield* fs.makeTempDirectoryScoped({ prefix: "storage-runtime-" });
          const stores = yield* Layer.build(
            platform.sessionStore({
              directory,
              namespace: "runtime",
              key: secret,
              ownership: "exclusive",
            }),
          );
          store = Context.get(stores, UploadSessionStore);
        } else {
          store = yield* UploadSessionStore.make({
            namespace: "runtime",
            ownership: "exclusive",
            protection,
          }).pipe(Effect.provide(KeyValueStore.layerMemory));
        }
        const sessions = yield* UploadSessions.make({ configurationId: "runtime-smoke-v1" }).pipe(
          Effect.provide(context),
          Effect.provideService(UploadSessionStore, store),
        );
        yield* sessions.createDownload({
          id: "download",
          ref: receipt.metadata.ref,
          expectedChecksum: receipt.checksum,
        });
        const output = [];
        const target = files
          ? yield* platform.fileDownloadTarget({
              id: "download",
              path: (yield* Path.Path).join(directory, "downloaded.bin"),
            })
          : {
              id: "download",
              prefix: (length) => Stream.succeed(Uint8Array.from(output.slice(0, length))),
              truncate: (length) =>
                Effect.sync(() => {
                  output.length = length;
                }),
              write: (offset, chunk) =>
                Effect.sync(() => {
                  for (let i = 0; i < chunk.length; i++) output[offset + i] = chunk[i];
                }),
              sync: Effect.void,
              publish: () => Effect.void,
            };
        yield* sessions.resumeDownload("download", target);
        check((yield* sessions.status("download")).state === "completed", "durable download state");
        checks.push("shared-runtime protected download session");

        if (files) {
          const path = yield* Path.Path;
          const fs = yield* FileSystem.FileSystem;
          const filename = path.join(directory, "downloaded.bin");
          check(
            [...(yield* fs.readFile(filename))].join(",") === [...bytes].join(","),
            "file publication",
          );
          const source = yield* UploadSource.file(filename);
          check(
            (yield* transfers.upload({ key: "file-source", source })).checksum.value === expected,
            "file source",
          );
          const backing = yield* platform.atomicFileStore(directory);
          check(
            Result.isFailure(yield* backing.keys.pipe(Effect.result)),
            "explicit enumeration limit",
          );
          const reconstructed = Context.get(
            yield* Layer.build(
              platform.sessionStore({
                directory,
                namespace: "runtime",
                key: secret,
                ownership: "exclusive",
              }),
            ),
            UploadSessionStore,
          );
          const recovered = yield* UploadSessions.make({
            configurationId: "runtime-smoke-v1",
          }).pipe(
            Effect.provide(context),
            Effect.provideService(UploadSessionStore, reconstructed),
          );
          check(
            (yield* recovered.status("download")).state === "completed",
            "file backing reconstruction",
          );
          yield* target.publish(receipt.checksum, bytes.length);
          checks.push(
            "real files, synced publication, replay source and reconstructed checkpoint backing",
          );
        }
        return { runtime: name, effect: "4.1.0", checks, resources: yield* transfers.resources };
      }),
    ).pipe(Effect.provide(platform.layer)),
  );
