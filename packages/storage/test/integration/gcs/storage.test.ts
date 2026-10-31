import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Stream } from "effect";

import { Storage as GoogleClient } from "@google-cloud/storage";

import { Storage } from "../../../src/core/storage.js";
import { GcsStorage } from "../../../src/providers/gcs/index.js";
import { runStorageConformance } from "../../../src/testing/index.js";
import { UploadSource } from "../../../src/transfers/index.js";
import { httpFixture } from "../../fixtures/gcs/http-fixture.js";
import { makeTransfers } from "../../fixtures/transfers/fixture.js";

it.effect("uses official SDK transport with lossless generations and same-response metadata", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* GcsStorage.make({
        bucket: "test-bucket",
        apiEndpoint: fixture.endpoint,
        useAuthWithCustomEndpoint: false,
      });
      const written = yield* storage.put({
        key: "a /?",
        body: new Uint8Array([1, 2, 3]),
        customMetadata: { test: "yes" },
      });
      assert.strictEqual(written.ref.version?.id, "9007199254740993");
      if (!written.etag) return assert.fail("Missing HTTP validator");
      const output = yield* storage.get({
        ref: written.ref,
        condition: { _tag: "IfMatch", etag: written.etag },
      });
      assert.strictEqual(output._tag, "Object");
      if (output._tag !== "Object") return;
      assert.strictEqual(output.metadata.ref.version?.id, "9007199254740993");
      assert.strictEqual(output.metadata.customMetadata?.test, "yes");
      assert.deepStrictEqual(
        Array.from((yield* Stream.runCollect(output.body)).flatMap((chunk) => Array.from(chunk))),
        [1, 2, 3],
      );
      const ranged = yield* storage.get({
        ref: written.ref,
        range: { _tag: "Bounded", start: 1, end: 2 },
      });
      assert.strictEqual(ranged._tag === "Object" && ranged.range?.totalSize, 3);
      const rejected = yield* storage
        .put({ key: "a /?", body: new Uint8Array(), condition: { _tag: "CreateOnly" } })
        .pipe(Effect.result);
      assert.strictEqual(
        rejected._tag === "Failure" && rejected.failure.reason._tag,
        "PreconditionFailed",
      );
      yield* storage.delete({ ref: written.ref });
    }),
  ),
);

it.effect("probes partially accepted offsets and resolves a lost final response", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* GcsStorage.make({
        bucket: "test-bucket",
        apiEndpoint: fixture.endpoint,
        useAuthWithCustomEndpoint: false,
      });
      const transfers = yield* makeTransfers(
        {
          storage,
          offsets: storage.offsets,
          maximumObjectBytes: 1024 * 1024,
          maximumSingleUploadBytes: 1024 * 1024,
          transportOverheadBytes: 1024,
        },
        {
          partBytes: 256 * 1024,
          maxBytes: 1024 * 1024,
          memoryBudgetBytes: 4 * 1024 * 1024,
          maxSourceChunkBytes: 1024 * 1024,
        },
      );
      fixture.partial();
      fixture.loseFinal();
      const receipt = yield* transfers.upload({
        key: "offset",
        source: UploadSource.bytes(new Uint8Array(600000).fill(7)),
        strategy: "offset",
      });
      assert.strictEqual(receipt.strategy, "offset");
      assert.strictEqual(receipt.metadata.size, 600000);
      assert.strictEqual(
        (yield* Storage.getBytes({ ref: receipt.metadata.ref, maxBytes: 1024 * 1024 }).pipe(
          Effect.provideService(Storage, storage),
        )).length,
        600000,
      );
      assert.isAtLeast(fixture.writes(), 3);
      const handle = yield* storage.offsets.create({ key: "cancel", contentLength: 1 });
      assert.isFalse(JSON.stringify(handle).includes(fixture.endpoint));
      yield* storage.offsets.cancel(handle);
      assert.strictEqual((yield* storage.offsets.query(handle))._tag, "Expired");
      const otherBucket = {
        ...handle,
        handle: Redacted.make(
          JSON.stringify({
            url: `${fixture.endpoint}/upload/storage/v1/b/other-bucket/o?uploadType=resumable&upload_id=1`,
            total: 1,
          }),
        ),
      };
      const rejectedBucket = yield* storage.offsets.query(otherBucket).pipe(Effect.result);
      assert.strictEqual(
        rejectedBucket._tag === "Failure" && rejectedBucket.failure.reason._tag,
        "InvalidSession",
      );
      const foreign = {
        ...handle,
        handle: Redacted.make(
          JSON.stringify({ url: "https://evil.example/upload/session/1", total: 1 }),
        ),
      };
      assert.strictEqual(
        (yield* storage.offsets.query(foreign).pipe(Effect.result))._tag,
        "Failure",
      );
    }),
  ),
);

it.effect("GCS validates full-object CRC32C without treating range hashes as range checksums", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* GcsStorage.make({
        bucket: "test-bucket",
        apiEndpoint: fixture.endpoint,
        useAuthWithCustomEndpoint: false,
      });
      yield* storage.put({ key: "integrity", body: new Uint8Array([1, 2, 3]) });
      const stored = fixture.objects.get("integrity");
      assert.isDefined(stored);
      if (!stored) return;
      stored.body[1] = 8;
      const response = yield* storage.get({ key: "integrity" });
      if (response._tag !== "Object") return assert.fail("Expected object");
      const result = yield* Stream.runDrain(response.body).pipe(Effect.result);
      assert.strictEqual(
        result._tag === "Failure" && result.failure.reason._tag,
        "IntegrityMismatch",
      );
      const range = yield* storage.get({
        key: "integrity",
        range: { _tag: "Bounded", start: 1, end: 1 },
      });
      if (range._tag !== "Object") return assert.fail("Expected range");
      yield* Stream.runDrain(range.body);
      assert.strictEqual(range.metadata.checksums?.[0]?.scope, "full-object");
    }),
  ),
);

it.effect(
  "GCS passes applicable portable contracts through SDK authorization and native JSON HTTP",
  () =>
    Effect.gen(function* () {
      const results = yield* runStorageConformance(() =>
        Effect.gen(function* () {
          const fixture = yield* httpFixture;
          return yield* GcsStorage.make({
            bucket: "test-bucket",
            apiEndpoint: fixture.endpoint,
            useAuthWithCustomEndpoint: false,
          });
        }),
      );
      assert.strictEqual(results.filter((result) => result.status === "passed").length, 12);
      assert.strictEqual(results.filter((result) => result.status === "skipped").length, 1);
    }),
);

it.effect(
  "empty offset uploads validate the source and expected digest before native initiation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        const storage = yield* GcsStorage.make({
          bucket: "test-bucket",
          apiEndpoint: fixture.endpoint,
          useAuthWithCustomEndpoint: false,
        });
        const transfers = yield* makeTransfers(
          {
            storage,
            offsets: storage.offsets,
            maximumObjectBytes: 1024 * 1024,
            maximumSingleUploadBytes: 1024 * 1024,
            transportOverheadBytes: 1024,
          },
          {
            partBytes: 256 * 1024,
            maxBytes: 1024 * 1024,
            memoryBudgetBytes: 4 * 1024 * 1024,
            maxSourceChunkBytes: 1024 * 1024,
          },
        );
        const invalid = yield* transfers
          .upload({
            key: "empty",
            strategy: "offset",
            source: UploadSource.bytes(new Uint8Array()),
            expectedChecksum: {
              algorithm: "sha256",
              encoding: "hex",
              scope: "full-object",
              value: "00",
            },
          })
          .pipe(Effect.result);
        assert.strictEqual(
          invalid._tag === "Failure" && invalid.failure.reason._tag,
          "IntegrityMismatch",
        );
        assert.strictEqual(fixture.sessions.size, 0);
        assert.strictEqual(
          (yield* transfers.upload({
            key: "empty",
            strategy: "offset",
            source: UploadSource.bytes(new Uint8Array()),
          })).metadata.size,
          0,
        );
      }),
    ),
);

it.effect(
  "GCS forwards the official SDK's native Headers and uses one exchange for primitive puts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        const bootstrap = new GoogleClient({ projectId: "fixture" });
        const authClient = bootstrap.authClient.fromAPIKey("local-fixture-api-key");
        const storage = yield* GcsStorage.make({
          bucket: "test-bucket",
          projectId: "fixture",
          apiEndpoint: fixture.endpoint,
          authClient,
          useAuthWithCustomEndpoint: true,
        });
        yield* storage.put({ key: "authenticated", body: new Uint8Array([1]) });
        assert.strictEqual(fixture.authenticatedRequests(), 1);
        yield* storage.head({ key: "authenticated" });
        assert.strictEqual(fixture.authenticatedRequests(), 2);
      }),
    ),
);

it.effect(
  "malformed final metadata remains an uncertain mutation and is reconciled without replay",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture;
        const storage = yield* GcsStorage.make({
          bucket: "test-bucket",
          apiEndpoint: fixture.endpoint,
          useAuthWithCustomEndpoint: false,
        });
        const transfers = yield* makeTransfers(
          {
            storage,
            offsets: storage.offsets,
            maximumObjectBytes: 1024 * 1024,
            maximumSingleUploadBytes: 1024 * 1024,
            transportOverheadBytes: 1024,
          },
          {
            partBytes: 256 * 1024,
            maxBytes: 1024 * 1024,
            memoryBudgetBytes: 4 * 1024 * 1024,
            maxSourceChunkBytes: 1024 * 1024,
          },
        );
        fixture.malformFinal();
        const receipt = yield* transfers.upload({
          key: "malformed",
          strategy: "offset",
          source: UploadSource.bytes(new Uint8Array([1])),
        });
        assert.strictEqual(receipt.metadata.size, 1);
        assert.strictEqual(fixture.writes(), 1);
      }),
    ),
);
