import { assert, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import { KeyValueStore } from "effect/persistence";

import { aes256Gcm } from "../../src/node.js";
import { UploadSessionStore } from "../../src/sessions.js";
import { UploadSource } from "../../src/transfers.js";
import { VercelStorage } from "../../src/vercel.js";
import { makeSessions } from "../sessions/fixture.js";
import { makeTransfers } from "../transfers/fixture.js";
import { httpFixture } from "./http-fixture.js";

const config = {
  storeId: "fixture",
  access: "private" as const,
  credential: { mode: "token" as const, token: Redacted.make("vercel_blob_rw_fixture_secret") },
};
const policy = {
  memoryBudgetBytes: 40 * 1024 ** 2,
  maxBytes: 10 * 1024 ** 2,
  maxSourceChunkBytes: 1024 ** 2,
  retryBaseMillis: 0,
};
it.effect("uses public manual multipart APIs with bounded parts and exact completion paths", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* VercelStorage.make(config);
      const transfers = yield* makeTransfers(
        {
          storage,
          multipart: storage.multipart,
          maximumObjectBytes: policy.maxBytes,
          maximumSingleUploadBytes: policy.maxBytes,
          transportOverheadBytes: 1024,
        },
        policy,
      );
      const result = yield* transfers.upload({
        key: "parts",
        source: UploadSource.bytes(new Uint8Array(9 * 1024 ** 2).fill(4)),
        strategy: "multipart",
      });
      assert.strictEqual(result.metadata.size, 9 * 1024 ** 2);
      assert.strictEqual(fixture.objects.get("parts")?.body.length, 9 * 1024 ** 2);
      assert.deepStrictEqual(
        fixture.seen
          .filter((entry) => entry.headers["x-mpu-action"] === "upload")
          .map((entry) => entry.headers["x-mpu-part-number"])
          .toSorted(),
        ["1", "2"],
      );
      assert.strictEqual(
        fixture.seen.find((entry) => entry.headers["x-mpu-action"] === "complete")?.headers[
          "x-add-random-suffix"
        ],
        "0",
      );
    }),
  ),
);

it.effect("fails durable upload admission before initiation when inventory is unavailable", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* VercelStorage.make(config);
      const store = yield* UploadSessionStore.make({
        namespace: "vercel",
        ownership: "exclusive",
        protection: aes256Gcm(Redacted.make(new Uint8Array(32).fill(1))),
      });
      const sessions = yield* makeSessions(
        {
          storage,
          multipart: storage.multipart,
          maximumObjectBytes: policy.maxBytes,
          maximumSingleUploadBytes: policy.maxBytes,
          transportOverheadBytes: 1024,
        },
        store,
        { configurationId: "vercel" },
        policy,
      );
      const result = yield* sessions
        .create({ key: "parts", source: UploadSource.bytes(new Uint8Array(9 * 1024 ** 2)) })
        .pipe(Effect.result);
      assert.strictEqual(result._tag, "Failure");
      assert.strictEqual(fixture.seen.length, 0);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("does not adopt a lost completion by pathname or pretend remote abort succeeded", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* VercelStorage.make(config);
      const upload = yield* storage.multipart.initiate({ key: "uncertain" });
      const part = yield* storage.multipart.uploadPart({
        upload,
        partNumber: 1,
        body: new Uint8Array([1]),
        contentLength: 1,
      });
      fixture.loseComplete();
      const result = yield* storage.multipart
        .complete({ upload, parts: [part] })
        .pipe(Effect.result);
      assert.strictEqual(result._tag === "Failure" && result.failure.mutationOutcome, "unknown");
      assert.strictEqual(fixture.objects.get("uncertain")?.body.length, 1);
      const inventory = yield* storage.multipart.listParts({ upload }).pipe(Effect.result);
      assert.strictEqual(
        inventory._tag === "Failure" && inventory.failure.reason._tag,
        "Unsupported",
      );
      const abort = yield* storage.multipart.abort(upload).pipe(Effect.result);
      assert.strictEqual(abort._tag === "Failure" && abort.failure.reason._tag, "Unsupported");
      assert.isTrue(fixture.objects.has("uncertain"));
    }),
  ),
);
