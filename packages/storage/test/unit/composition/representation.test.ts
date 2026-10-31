import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Stream } from "effect";

import { generateX25519Identity, identityToRecipient } from "age-encryption";

import { StorageCompression } from "../../../src/composition/compression.js";
import { AgeEncryption } from "../../../src/composition/encryption/age.js";
import {
  StorageEncryption,
  StorageEncryptionCodec,
} from "../../../src/composition/encryption/index.js";
import { Storage } from "../../../src/core/storage.js";
import { layer as platform } from "../../../src/runtime/node.js";
import { MemoryStorage } from "../../../src/testing/index.js";

const limits = { maxDecodedBytes: 1024 * 1024, maxStoredBytes: 2 * 1024 * 1024 };
const read = (storage: Storage["Service"], key: string) =>
  Storage.getBytes({ key, maxBytes: limits.maxDecodedBytes }).pipe(
    Effect.provideService(Storage, storage),
  );

it.live(
  "gzip stores an explicit representation and returns verified logical metadata and bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const source = yield* MemoryStorage.make();
        const storage = yield* StorageCompression.make({
          ...limits,
          format: "gzip",
          maxExpansionRatio: 10000,
        }).pipe(Effect.provideService(Storage, source));
        const bytes = new TextEncoder().encode("compressible ".repeat(10000));
        const receipt = yield* storage.put({
          key: "file",
          body: Stream.make(bytes.subarray(0, 100), bytes.subarray(100)),
          contentLength: bytes.length,
          contentType: "text/plain",
          customMetadata: { purpose: "test" },
        });
        const stored = yield* source.head({ key: "file" });
        assert.isBelow(stored.size ?? Infinity, bytes.length);
        assert.strictEqual(receipt.size, bytes.length);
        assert.strictEqual((yield* storage.head({ key: "file" })).customMetadata?.purpose, "test");
        assert.deepStrictEqual(yield* read(storage, "file"), bytes);
        assert.strictEqual(
          (yield* Effect.result(storage.get({ key: "file", range: { _tag: "From", start: 1 } })))
            ._tag,
          "Failure",
        );
        const notModified = yield* storage.get({
          key: "file",
          condition: { _tag: "IfNoneMatch", etag: receipt.etag ?? "" },
        });
        assert.strictEqual(notModified._tag, "NotModified");
      }),
    ).pipe(Effect.provide(platform)),
);

it.live("gzip rejects corruption, expansion limits and changed object context", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const source = yield* MemoryStorage.make();
      const storage = yield* StorageCompression.make({ ...limits, format: "gzip" }).pipe(
        Effect.provideService(Storage, source),
      );
      yield* storage.put({ key: "a", body: new Uint8Array(100000) });
      const strict = yield* StorageCompression.make({
        ...limits,
        format: "gzip",
        maxExpansionRatio: 2,
      }).pipe(Effect.provideService(Storage, source));
      assert.strictEqual((yield* Effect.result(strict.head({ key: "a" })))._tag, "Failure");
      const original = yield* source.get({ key: "a" });
      if (original._tag !== "Object") return assert.fail("Missing object");
      const encoded = yield* Storage.getBytes({ key: "a", maxBytes: limits.maxStoredBytes }).pipe(
        Effect.provideService(Storage, source),
      );
      yield* source.put({
        key: "b",
        body: encoded,
        customMetadata: original.metadata.customMetadata,
      });
      assert.strictEqual((yield* Effect.result(storage.head({ key: "b" })))._tag, "Failure");
      encoded[encoded.length - 1] = (encoded[encoded.length - 1] ?? 0) ^ 1;
      yield* source.put({
        key: "a",
        body: encoded,
        customMetadata: original.metadata.customMetadata,
      });
      assert.strictEqual((yield* Effect.result(storage.head({ key: "a" })))._tag, "Failure");
    }),
  ).pipe(Effect.provide(platform)),
);

it.live(
  "age encrypts content and metadata, rejects wrong identities/tampering and supports retained rotation keys",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const identity = yield* Effect.promise(() => generateX25519Identity());
        const recipient = yield* Effect.promise(() => identityToRecipient(identity));
        const another = yield* Effect.promise(() => generateX25519Identity());
        const source = yield* MemoryStorage.make();
        const codec = yield* AgeEncryption.make({
          recipients: [recipient],
          identities: Effect.succeed([Redacted.make(identity)]),
        });
        const storage = yield* StorageEncryption.make(limits).pipe(
          Effect.provideService(StorageEncryptionCodec, codec),
          Effect.provideService(Storage, source),
        );
        const message = new TextEncoder().encode("private content");
        yield* storage.put({
          key: "private",
          body: message,
          contentType: "text/plain",
          customMetadata: { secret: "private metadata" },
        });
        const physical = yield* Storage.getBytes({
          key: "private",
          maxBytes: limits.maxStoredBytes,
        }).pipe(Effect.provideService(Storage, source));
        assert.isFalse(new TextDecoder().decode(physical).includes("private content"));
        assert.isFalse(
          JSON.stringify(yield* source.head({ key: "private" })).includes("private metadata"),
        );
        assert.deepStrictEqual(yield* read(storage, "private"), message);
        const wrong = yield* AgeEncryption.make({
          recipients: [recipient],
          identities: Effect.succeed([Redacted.make(another)]),
        });
        const wrongStorage = yield* StorageEncryption.make(limits).pipe(
          Effect.provideService(StorageEncryptionCodec, wrong),
          Effect.provideService(Storage, source),
        );
        assert.strictEqual(
          (yield* Effect.result(wrongStorage.head({ key: "private" })))._tag,
          "Failure",
        );
        const rotated = yield* AgeEncryption.make({
          recipients: [recipient],
          identities: Effect.succeed([Redacted.make(another), Redacted.make(identity)]),
        });
        const rotatedStorage = yield* StorageEncryption.make(limits).pipe(
          Effect.provideService(StorageEncryptionCodec, rotated),
          Effect.provideService(Storage, source),
        );
        assert.deepStrictEqual(yield* read(rotatedStorage, "private"), message);
        yield* source.put({
          key: "rebound",
          body: physical,
          customMetadata: { "effect-storage-representation": codec.format },
        });
        const rebound = yield* Effect.result(storage.head({ key: "rebound" }));
        assert.equal(rebound._tag, "Failure");
        if (rebound._tag === "Failure")
          assert.equal(rebound.failure.reason._tag, "IntegrityMismatch");

        for (const length of [0, Math.floor(physical.length / 2), physical.length - 1]) {
          yield* source.put({
            key: "private",
            body: physical.subarray(0, length),
            customMetadata: { "effect-storage-representation": codec.format },
          });
          const truncated = yield* Effect.result(storage.head({ key: "private" }));
          assert.equal(truncated._tag, "Failure");
          if (truncated._tag === "Failure")
            assert.equal(truncated.failure.reason._tag, "IntegrityMismatch");
        }
        physical[physical.length - 1] = (physical[physical.length - 1] ?? 0) ^ 1;
        yield* source.put({
          key: "private",
          body: physical,
          customMetadata: { "effect-storage-representation": codec.format },
        });
        assert.strictEqual(
          (yield* Effect.result(storage.head({ key: "private" })))._tag,
          "Failure",
        );
      }),
    ).pipe(Effect.provide(platform)),
);

it.live("representation staging preserves caller stream failures before provider writes", () =>
  Effect.gen(function* () {
    const source = yield* MemoryStorage.make();
    const storage = yield* StorageCompression.make({ ...limits, format: "gzip" }).pipe(
      Effect.provideService(Storage, source),
    );
    const outcome = yield* Effect.result(
      storage.put({ key: "uncommitted", body: Stream.fail("source-failed" as const) }),
    );
    assert.strictEqual(outcome._tag, "Failure");
    if (outcome._tag === "Failure") assert.strictEqual(outcome.failure, "source-failed");
    assert.strictEqual((yield* source.listPage()).entries.length, 0);
  }).pipe(Effect.provide(platform)),
);
