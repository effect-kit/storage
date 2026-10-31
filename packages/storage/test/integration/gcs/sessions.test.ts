import { assert, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";
import { KeyValueStore } from "effect/persistence";

import { storageError } from "../../../src/core/errors.js";
import { GcsStorage } from "../../../src/providers/gcs/index.js";
import { aes256Gcm } from "../../../src/runtime/node.js";
import { UploadSessionStore } from "../../../src/sessions/index.js";
import { UploadSource } from "../../../src/transfers/index.js";
import { httpFixture } from "../../fixtures/gcs/http-fixture.js";
import { makeSessions } from "../../fixtures/sessions/fixture.js";

it.effect("GCS checkpoints protect session URLs and recover offsets with a new service", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* GcsStorage.make({
        bucket: "test-bucket",
        apiEndpoint: fixture.endpoint,
        useAuthWithCustomEndpoint: false,
      });
      const endpoint = {
        storage,
        offsets: storage.offsets,
        maximumObjectBytes: 1024 * 1024,
        maximumSingleUploadBytes: 1024 * 1024,
        transportOverheadBytes: 1024,
      };
      const policy = {
        partBytes: 256 * 1024,
        maxBytes: 1024 * 1024,
        maxSourceChunkBytes: 1024 * 1024,
        memoryBudgetBytes: 4 * 1024 * 1024,
        retryBaseMillis: 0,
      };
      const store = yield* UploadSessionStore.make({
        namespace: "native",
        ownership: "exclusive",
        protection: aes256Gcm(Redacted.make(new Uint8Array(32).fill(1))),
      });
      const source = UploadSource.bytes(new Uint8Array(600000).fill(7));
      let count = 0;
      const first = yield* makeSessions(
        {
          ...endpoint,
          offsets: {
            ...storage.offsets,
            write: (request) =>
              ++count === 2
                ? Effect.fail(
                    storageError(
                      "write",
                      "gcs",
                      { _tag: "ProviderFailure", code: "interrupted" },
                      "rejected",
                    ),
                  )
                : storage.offsets.write(request),
          },
        },
        store,
        { configurationId: "offset" },
        policy,
      );
      const id = yield* first.create({ key: "restart", source });
      assert.strictEqual((yield* first.resume(id, source).pipe(Effect.result))._tag, "Failure");
      const checkpoint = yield* store.read(id);
      assert.strictEqual(checkpoint?.kind === "upload" && checkpoint.offset, 256 * 1024);
      const kvs = yield* KeyValueStore.KeyValueStore;
      const raw = yield* kvs.get(`native:${id}`);
      assert.isFalse(raw?.includes(fixture.endpoint));
      const restarted = yield* makeSessions(endpoint, store, { configurationId: "offset" }, policy);
      const before = fixture.writes();
      assert.strictEqual(
        (yield* restarted
          .resume(id, UploadSource.bytes(new Uint8Array(600000).fill(8)))
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.strictEqual(fixture.writes(), before);
      fixture.loseFinal();
      const receipt = yield* restarted.resume(id, source);
      assert.strictEqual(receipt.size, 600000);
      assert.strictEqual((yield* restarted.status(id)).state, "completed");
      assert.strictEqual(fixture.sessions.size, 1);
    }),
  ).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
