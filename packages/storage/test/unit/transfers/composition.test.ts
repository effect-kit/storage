import { assert, it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Result, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";
import { TestClock } from "effect/testing";

import { Storage } from "../../../src/core/storage.js";
import { UploadSessions, UploadSessionStore } from "../../../src/sessions/index.js";
import { Transfers, UploadSource } from "../../../src/transfers/index.js";
import { makeSessions, setup } from "../../fixtures/sessions/fixture.js";
import { fixture, policy, withEndpoint } from "../../fixtures/transfers/fixture.js";

it.effect(
  "managed uploads and sessions share memory admission without nesting verification leases",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const context = yield* withEndpoint(
        Layer.build(Transfers.layer({ ...policy, memoryBudgetBytes: 100 * 1024 })),
        test.endpoint,
      );
      const transfers = Context.get(context, Transfers);
      const sessions = yield* UploadSessions.make({ configurationId: "shared" }).pipe(
        Effect.provide(context),
        Effect.provideService(UploadSessionStore, test.store),
      );
      const entered = Deferred.makeUnsafe<void>();
      const release = Deferred.makeUnsafe<void>();
      const upload = yield* transfers
        .upload({
          key: "managed",
          source: UploadSource.stream(
            Stream.fromEffect(
              Effect.gen(function* () {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
                return new Uint8Array(32);
              }),
            ),
            32,
          ),
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const source = UploadSource.bytes(new Uint8Array(32));
      const creation = yield* sessions.create({ key: "durable", source }).pipe(Effect.forkChild);
      yield* TestClock.adjust(1);
      assert.isUndefined(creation.pollUnsafe());

      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(upload);
      const id = yield* Fiber.join(creation);
      yield* sessions.resume(id, source);
      assert.strictEqual((yield* sessions.status(id)).state, "completed");
      const resources = yield* transfers.resources;
      assert.isAtMost(resources.peakReservedBytes, 100 * 1024);
      assert.strictEqual(resources.reservedBytes, 0);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("session protocol requests wait for a managed download's shared request permit", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const context = yield* withEndpoint(
      Layer.build(Transfers.layer({ ...policy, requestConcurrency: 1 })),
      test.endpoint,
    );
    const transfers = Context.get(context, Transfers);
    const sessions = yield* UploadSessions.make({ configurationId: "shared" }).pipe(
      Effect.provide(context),
      Effect.provideService(UploadSessionStore, test.store),
    );
    const source = UploadSource.bytes(new Uint8Array(32));
    const id = yield* sessions.create({ key: "durable", source });
    const metadata = yield* test.endpoint.storage.put({ key: "read", body: new Uint8Array(1) });
    const entered = Deferred.makeUnsafe<void>();
    const release = Deferred.makeUnsafe<void>();
    const download = yield* transfers
      .download({
        ref: metadata.ref,
        target: {
          write: () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        },
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    const upload = yield* sessions.resume(id, source).pipe(Effect.forkChild);
    yield* TestClock.adjust(1);
    assert.strictEqual(test.counts().initiates, 0);
    assert.isUndefined(upload.pollUnsafe());
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(download);
    yield* Fiber.join(upload);
    assert.strictEqual((yield* transfers.resources).peakActiveRequests, 1);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("a driver from another Storage instance is rejected at construction", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const result = yield* withEndpoint(
      Transfers.make(policy).pipe(Effect.provideService(Storage, { ...test.endpoint.storage })),
      test.endpoint,
    ).pipe(Effect.result);
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result))
      assert.strictEqual(result.failure.reason._tag, "InvalidConfiguration");
  }),
);

it.effect("missing durable upload capability is rejected without opening the source", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const sessions = yield* makeSessions({ ...test.endpoint, handles: undefined }, test.store, {
      configurationId: "no-durable-upload",
    });
    let opened = false;
    const source = UploadSource.replayable(() => {
      opened = true;
      return Stream.empty;
    }, 0);
    assert.isTrue(
      Result.isFailure(yield* sessions.create({ key: "unsupported", source }).pipe(Effect.result)),
    );
    assert.isFalse(opened);
    assert.strictEqual(test.counts().initiates, 0);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
