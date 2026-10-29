import { assert, it } from "@effect/vitest";
import { Effect, Result, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { storageError } from "../../src/errors.js";
import { UploadSource } from "../../src/transfers.js";
import { setup, makeSessions } from "./fixture.js";

it.effect("persists protected checkpoints and resumes acknowledged parts with a new service", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const sessions = yield* test.make();
    const source = UploadSource.bytes(new Uint8Array(48).fill(7));
    const id = yield* sessions.create({ key: "private-key", source });
    let calls = 0;
    const failing = yield* test.make({
      ...test.endpoint,
      multipart: {
        ...test.endpoint.multipart,
        uploadPart: (request) =>
          ++calls === 2
            ? Effect.fail(storageError("part", "test", { _tag: "TransportFailure" }, "unknown"))
            : test.endpoint.multipart.uploadPart(request),
      },
    });
    assert.isTrue(Result.isFailure(yield* failing.resume(id, source).pipe(Effect.result)));
    assert.strictEqual((yield* sessions.status(id)).completedParts, 1);
    const restarted = yield* test.make();
    const metadata = yield* restarted.resume(id, source);
    assert.strictEqual(metadata?.size, 48);
    assert.strictEqual(test.counts().partAttempts, 3);
    assert.strictEqual((yield* restarted.status(id)).state, "completed");
    const raw = yield* test.kvs.get(`test:${id}`);
    assert.isFalse(raw?.includes("private-key"));
    assert.isFalse(raw?.includes("test-upload"));
    assert.isFalse(JSON.stringify(yield* restarted.status(id)).includes("uploadId"));
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("lost completion reconciles without completing or initiating twice", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const source = UploadSource.bytes(new Uint8Array(32));
    const sessions = yield* test.make({
      ...test.endpoint,
      multipart: {
        ...test.endpoint.multipart,
        complete: (request) =>
          test.endpoint.multipart
            .complete(request)
            .pipe(
              Effect.andThen(
                Effect.fail(
                  storageError("complete", "test", { _tag: "TransportFailure" }, "unknown"),
                ),
              ),
            ),
      },
    });
    const id = yield* sessions.create({ key: "lost", source });
    assert.isTrue(Result.isFailure(yield* sessions.resume(id, source).pipe(Effect.result)));
    assert.strictEqual((yield* sessions.status(id)).state, "completing");
    assert.isTrue(Result.isFailure(yield* sessions.resume(id, source).pipe(Effect.result)));
    assert.strictEqual((yield* sessions.reconcile(id)).state, "verifying");
    yield* (yield* test.make()).resume(id, source);
    assert.strictEqual(test.counts().initiates, 1);
    assert.strictEqual(test.counts().completes, 1);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "changed sources and configuration fail without publishing, and event delivery retries a stable id",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const sessions = yield* test.make();
      const id = yield* sessions.create({
        key: "events",
        source: UploadSource.bytes(new Uint8Array([1])),
      });
      assert.isTrue(
        Result.isFailure(
          yield* sessions.resume(id, UploadSource.bytes(new Uint8Array([2]))).pipe(Effect.result),
        ),
      );
      assert.strictEqual(test.counts().initiates, 0);
      const foreign = yield* makeSessions(test.endpoint, test.store, {
        configurationId: "different",
      });
      assert.isTrue(Result.isFailure(yield* foreign.status(id).pipe(Effect.result)));
      yield* sessions.resume(id, UploadSource.bytes(new Uint8Array([1])));
      const delivered: string[] = [];
      yield* sessions
        .deliverEvents(id, (event) =>
          Effect.sync(() => {
            delivered.push(event.id);
          }).pipe(
            Effect.andThen(
              Effect.fail(storageError("event", "test", { _tag: "TransportFailure" })),
            ),
          ),
        )
        .pipe(Effect.result);
      yield* sessions.deliverEvents(id, (event) =>
        Effect.sync(() => {
          delivered.push(event.id);
        }),
      );
      yield* sessions.deliverEvents(id, () => Effect.die("already delivered"));
      assert.strictEqual(delivered.length, 2);
      assert.strictEqual(delivered[0], delivered[1]);
      assert.isTrue(delivered[0]?.endsWith(":completed"));
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("restartable downloads discard uncheckpointed tails and verify the existing prefix", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const metadata = yield* test.endpoint.storage.put({
      key: "download",
      body: new Uint8Array([1, 2, 3, 4]),
    });
    const sessions = yield* test.make();
    const id = "download-target";
    yield* sessions.createDownload({ id, ref: metadata.ref });
    let bytes: number[] = [];
    let fail = true;
    let published = false;
    const target = {
      id,
      prefix: (length: number) => Stream.succeed(Uint8Array.from(bytes.slice(0, length))),
      truncate: (length: number) =>
        Effect.sync(() => {
          bytes = bytes.slice(0, length);
        }),
      write: (_offset: number, chunk: Uint8Array) =>
        Effect.sync(() => {
          bytes.push(...chunk);
        }).pipe(
          Effect.flatMap(() =>
            fail
              ? Effect.fail(storageError("target", "test", { _tag: "TransportFailure" }))
              : Effect.void,
          ),
        ),
      sync: Effect.void,
      publish: () =>
        Effect.sync(() => {
          published = true;
        }),
    };
    assert.isTrue(Result.isFailure(yield* sessions.resumeDownload(id, target).pipe(Effect.result)));
    assert.deepEqual(bytes, [1, 2, 3, 4]);
    assert.strictEqual((yield* sessions.status(id)).offset, 0);
    fail = false;
    yield* (yield* test.make()).resumeDownload(id, target);
    assert.deepEqual(bytes, [1, 2, 3, 4]);
    assert.isTrue(published);
    assert.strictEqual((yield* sessions.status(id)).state, "completed");
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "definitive completion rejection permits retry while cleanup keeps its original error",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      let completed = 0;
      const denied = storageError("complete", "test", { _tag: "Throttled" }, "rejected");
      const sessions = yield* test.make({
        ...test.endpoint,
        multipart: {
          ...test.endpoint.multipart,
          complete: (request) =>
            ++completed === 1 ? Effect.fail(denied) : test.endpoint.multipart.complete(request),
        },
      });
      const source = UploadSource.bytes(new Uint8Array(32));
      const id = yield* sessions.create({ key: "retry-completion", source });
      const first = yield* sessions.resume(id, source).pipe(Effect.result);
      assert.isTrue(Result.isFailure(first) && first.failure === denied);
      assert.strictEqual((yield* sessions.status(id)).state, "uploading");
      yield* sessions.resume(id, source);
      assert.strictEqual(test.counts().completes, 1);

      const abortDenied = storageError("abort", "test", { _tag: "AccessDenied" }, "rejected");
      const cleanup = yield* test.make({
        ...test.endpoint,
        multipart: {
          ...test.endpoint.multipart,
          uploadPart: () => Effect.fail(denied),
          abort: () => Effect.fail(abortDenied),
        },
      });
      const other = yield* cleanup.create({ key: "cleanup", source });
      yield* cleanup.resume(other, source).pipe(Effect.result);
      const failure = yield* cleanup.abort(other).pipe(Effect.result);
      assert.isTrue(Result.isFailure(failure) && failure.failure === abortDenied);
      assert.strictEqual((yield* cleanup.status(other)).state, "cleanup-pending");
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "a changed source body cannot make future recovery adopt its mismatched part receipts",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const sessions = yield* test.make();
      let opens = 0;
      const changing = UploadSource.replayable(
        () => Stream.succeed(new Uint8Array(32).fill(++opens === 3 ? 9 : 0)),
        32,
      );
      const id = yield* sessions.create({ key: "changed-during-read", source: changing });
      assert.isTrue(Result.isFailure(yield* sessions.resume(id, changing).pipe(Effect.result)));
      assert.strictEqual(test.counts().completes, 0);
      yield* sessions.resume(id, UploadSource.bytes(new Uint8Array(32)));
      assert.strictEqual(test.counts().partAttempts, 4);
      assert.strictEqual((yield* sessions.status(id)).state, "completed");
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("download ownership cannot change during recovery and cleanup never publishes", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const sessions = yield* test.make();
    const metadata = yield* test.endpoint.storage.put({
      key: "target-identity",
      body: new Uint8Array([1]),
    });
    const id = yield* sessions.createDownload({ id: "partial", ref: metadata.ref });
    let discarded = false;
    const target = {
      id,
      identity: "original-file",
      prefix: () => Stream.empty,
      truncate: () => Effect.void,
      write: () => Effect.fail("sink failure"),
      sync: Effect.void,
      publish: () => Effect.die("not verified"),
      discard: () =>
        Effect.sync(() => {
          discarded = true;
        }),
    };
    yield* sessions.resumeDownload(id, target).pipe(Effect.result);
    const changed = { ...target, identity: "another-file" };
    assert.isTrue(Result.isFailure(yield* sessions.abortDownload(id, changed).pipe(Effect.result)));
    assert.isFalse(discarded);
    yield* sessions.abortDownload(id, target);
    assert.isTrue(discarded);
    assert.strictEqual((yield* sessions.status(id)).state, "aborted");
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "publication response loss stays unknown and a verified target can be recognized on retry",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const sessions = yield* test.make();
      const metadata = yield* test.endpoint.storage.put({
        key: "publication",
        body: new Uint8Array([1, 2]),
      });
      const id = yield* sessions.createDownload({ id: "publication-target", ref: metadata.ref });
      let published = false;
      const target = {
        id,
        prefix: () => Stream.empty,
        truncate: () => Effect.void,
        write: () => Effect.void,
        sync: Effect.void,
        publish: () =>
          Effect.suspend(() => {
            if (published) return Effect.void;
            published = true;
            return Effect.fail(
              storageError("publish", "test", { _tag: "TransportFailure" }, "not-started"),
            );
          }),
      };
      const lost = yield* sessions.resumeDownload(id, target).pipe(Effect.result);
      assert.isTrue(Result.isFailure(lost) && lost.failure.mutationOutcome === "unknown");
      assert.strictEqual((yield* sessions.status(id)).state, "verified");
      yield* sessions.resumeDownload(id, target);
      assert.strictEqual((yield* sessions.status(id)).state, "completed");
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
