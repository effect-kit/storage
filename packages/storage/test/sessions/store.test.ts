import { assert, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Redacted, Result, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";
import { TestClock } from "effect/testing";

import { UploadSessionStore } from "../../src/sessions.js";
import { UploadSource } from "../../src/transfers.js";
import { setup, makeSessions } from "./fixture.js";

it.effect("corrupt and future-version records fail without deletion or remote mutation", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const sessions = yield* test.make();
    const source = UploadSource.bytes(new Uint8Array(16));
    const id = yield* sessions.create({ key: "record", source });
    const key = `test:${id}`;
    const original = yield* test.kvs.get(key);
    assert.isDefined(original);
    for (const corrupt of ['{"version":2}', "not-json", '{"version":1,"ciphertext":"bad"}']) {
      yield* test.kvs.set(key, corrupt);
      assert.isTrue(Result.isFailure(yield* sessions.status(id).pipe(Effect.result)));
      assert.strictEqual(yield* test.kvs.get(key), corrupt);
    }
    yield* test.kvs.set(key, original ?? "");
    const envelope = JSON.parse(original ?? "") as { ciphertext: string };
    const plaintext = yield* test.protection.open(Redacted.make(envelope.ciphertext), key);
    const future = { ...(JSON.parse(plaintext) as object), version: 2 };
    const ciphertext = yield* test.protection.seal(JSON.stringify(future), key);
    yield* test.kvs.set(
      key,
      JSON.stringify({ version: 1, ciphertext: Redacted.value(ciphertext) }),
    );
    assert.isTrue(Result.isFailure(yield* sessions.resume(id, source).pipe(Effect.result)));
    assert.strictEqual(test.counts().initiates, 0);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("namespace-bound protection rejects copying ciphertext to another session", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const sessions = yield* test.make();
    const id = yield* sessions.create({
      key: "identity",
      source: UploadSource.bytes(new Uint8Array()),
    });
    yield* test.kvs.set("test:other", (yield* test.kvs.get(`test:${id}`)) ?? "");
    assert.isTrue(Result.isFailure(yield* sessions.status("other").pipe(Effect.result)));
    const otherStore = yield* UploadSessionStore.make({
      namespace: "other",
      ownership: "exclusive",
      protection: test.protection,
    }).pipe(Effect.provideService(KeyValueStore.KeyValueStore, test.kvs));
    assert.isUndefined(yield* otherStore.read(id));
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("two service instances serialize finalization and reject stale revisions", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const first = yield* test.make();
    const secondStore = yield* UploadSessionStore.make({
      namespace: "test",
      ownership: "exclusive",
      protection: test.protection,
    }).pipe(Effect.provideService(KeyValueStore.KeyValueStore, test.kvs));
    const second = yield* makeSessions(test.endpoint, secondStore, {
      configurationId: "test-config",
    });
    const source = UploadSource.bytes(new Uint8Array(32));
    const id = yield* first.create({ key: "competing", source });
    const original = yield* test.store.read(id);
    assert.isDefined(original);
    yield* Effect.all([first.resume(id, source), second.resume(id, source)], { concurrency: 2 });
    assert.strictEqual(test.counts().completes, 1);
    if (original)
      assert.isTrue(
        Result.isFailure(
          yield* test.store
            .exclusive(test.store.write(original, original.revision))
            .pipe(Effect.result),
        ),
      );
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("expired sessions reject work but remain inspectable and cleanable", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const sessions = yield* makeSessions(test.endpoint, test.store, {
      configurationId: "test-config",
      expiresInMillis: 10,
    });
    const source = UploadSource.bytes(new Uint8Array(1));
    const id = yield* sessions.create({ key: "expiry", source });
    yield* TestClock.adjust(10);
    assert.isTrue((yield* sessions.status(id)).expired);
    assert.isTrue(Result.isFailure(yield* sessions.resume(id, source).pipe(Effect.result)));
    yield* sessions.abort(id);
    assert.strictEqual((yield* sessions.status(id)).state, "aborted");
    assert.strictEqual(test.counts().initiates, 0);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "one-shot sources cannot create restart promises and failed batch entries retain completed peers",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const sessions = yield* test.make();
      assert.isTrue(
        Result.isFailure(
          yield* sessions
            .create({
              key: "one-shot",
              source: UploadSource.stream(Stream.succeed(new Uint8Array(1)), 1),
            })
            .pipe(Effect.result),
        ),
      );
      const first = yield* sessions.create({
        key: "first",
        source: UploadSource.bytes(new Uint8Array([1])),
      });
      const second = yield* sessions.create({
        key: "second",
        source: UploadSource.bytes(new Uint8Array([2])),
      });
      const batch = yield* sessions.createBatch([first, second]);
      const result = yield* sessions.processBatch(batch, (id) =>
        sessions.resume(id, UploadSource.bytes(new Uint8Array([2]))),
      );
      assert.deepEqual(
        result.map((entry) => entry.state),
        ["failed", "completed"],
      );
      const restarted = yield* test.make();
      const finished = yield* restarted.processBatch(batch, (id) =>
        restarted.resume(id, UploadSource.bytes(new Uint8Array([id === first ? 1 : 2]))),
      );
      assert.isTrue(finished.every((entry) => entry.state === "completed"));
      assert.strictEqual(test.counts().completes, 2);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "retention requires terminal state and delivered events before removing a checkpoint",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const sessions = yield* test.make();
      const source = UploadSource.bytes(new Uint8Array(1));
      const id = yield* sessions.create({ key: "retention", source });
      assert.isTrue(Result.isFailure(yield* sessions.forget(id).pipe(Effect.result)));
      yield* sessions.resume(id, source);
      assert.isTrue(Result.isFailure(yield* sessions.forget(id).pipe(Effect.result)));
      yield* sessions.deliverEvents(id, () => Effect.void);
      yield* sessions.forget(id);
      assert.isUndefined(yield* test.store.read(id));
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("read-only status remains available while a transfer owns the mutation scope", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const sessions = yield* test.make({
      ...test.endpoint,
      multipart: {
        ...test.endpoint.multipart,
        uploadPart: (request) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            return yield* test.endpoint.multipart.uploadPart(request);
          }),
      },
    });
    const source = UploadSource.bytes(new Uint8Array(16));
    const id = yield* sessions.create({ key: "polling", source });
    const transfer = yield* sessions.resume(id, source).pipe(Effect.forkChild);
    yield* Deferred.await(entered);
    const polling = yield* sessions.status(id).pipe(Effect.forkChild);
    yield* TestClock.adjust(1);
    const result = yield* Effect.sync(() => polling.pollUnsafe());
    assert.strictEqual(result?._tag, "Success");
    if (result?._tag === "Success") assert.strictEqual(result.value.state, "uploading");
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(transfer);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
