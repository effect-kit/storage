import { assert, it } from "@effect/vitest";
import { Effect, Redacted, Result } from "effect";
import { KeyValueStore } from "effect/persistence";
import { TestClock } from "effect/testing";

import { Storage } from "../../../src/core/storage.js";
import { failUpload } from "../../../src/uploads/schema.js";
import { actor, input, setup } from "../../fixtures/uploads/fixture.js";
const bytes = new TextEncoder().encode("hello");

it.effect(
  "upload ownership and idempotency bind actor, tenant, request and authoritative destination",
  () =>
    Effect.gen(function* () {
      const { uploads } = yield* setup();
      const created = yield* uploads.create({ actor, input, idempotencyKey: "same-request" });
      assert.strictEqual(
        created.id,
        (yield* uploads.create({ actor, input, idempotencyKey: "same-request" })).id,
      );
      const changed = yield* Effect.result(
        uploads.create({
          actor,
          input: { ...input, context: { documentId: "other" } },
          idempotencyKey: "same-request",
        }),
      );
      assert.isTrue(
        Result.isFailure(changed) &&
          changed.failure._tag === "UploadError" &&
          changed.failure.code === "Conflict",
      );
      const foreign = yield* Effect.result(
        uploads.status({ actor: { ...actor, tenantId: "other" }, uploadId: created.id }),
      );
      assert.isTrue(
        Result.isFailure(foreign) &&
          foreign.failure._tag === "UploadError" &&
          foreign.failure.code === "NotFound",
      );
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "verification snapshots published bytes so replaying staging authority cannot change an attachment",
  () =>
    Effect.gen(function* () {
      let publications = 0;
      const test = yield* setup({
        publish: () =>
          Effect.sync(() => {
            publications++;
            return { attachmentId: "attachment" };
          }),
      });
      const created = yield* test.uploads.create({ actor, input, idempotencyKey: "one" });
      yield* test.uploads.content({ actor, uploadId: created.id, body: bytes });
      const result = yield* test.uploads.complete({ actor, uploadId: created.id });
      assert.strictEqual(result.state, "published");
      const record = yield* test.store.read(created.id);
      if (!record?.object) return assert.fail("Missing pinned publication");
      yield* test.storage.put({ key: record.stagingKey, body: new TextEncoder().encode("evil!") });
      assert.strictEqual(
        yield* Storage.getText({ ref: record.object.ref, maxBytes: 5 }).pipe(
          Effect.provideService(Storage, test.storage),
        ),
        "hello",
      );
      yield* test.uploads.complete({ actor, uploadId: created.id });
      assert.strictEqual(publications, 1);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("a wrong checksum and failed content inspection never reach publication", () =>
  Effect.gen(function* () {
    let publications = 0;
    const test = yield* setup({
      publish: () =>
        Effect.sync(() => {
          publications++;
          return {};
        }),
    });
    const created = yield* test.uploads.create({ actor, input, idempotencyKey: "bad-hash" });
    yield* test.uploads.content({
      actor,
      uploadId: created.id,
      body: new TextEncoder().encode("wrong"),
    });
    const result = yield* Effect.result(test.uploads.complete({ actor, uploadId: created.id }));
    assert.isTrue(
      Result.isFailure(result) &&
        result.failure._tag === "UploadError" &&
        result.failure.code === "VerificationFailed",
    );
    assert.strictEqual(
      (yield* test.uploads.status({ actor, uploadId: created.id })).state,
      "rejected",
    );
    const inspecting = yield* setup({
      inspect: () => Effect.fail(failUpload("VerificationFailed")),
      publish: () =>
        Effect.sync(() => {
          publications++;
          return {};
        }),
    });
    const second = yield* inspecting.uploads.create({
      actor,
      input,
      idempotencyKey: "bad-content",
    });
    yield* inspecting.uploads.content({ actor, uploadId: second.id, body: bytes });
    assert.isTrue(
      Result.isFailure(
        yield* Effect.result(inspecting.uploads.complete({ actor, uploadId: second.id })),
      ),
    );
    assert.strictEqual(publications, 0);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("lost publication responses recover with stable application idempotency keys", () =>
  Effect.gen(function* () {
    const transactions = new Set<string>();
    let calls = 0;
    const test = yield* setup({
      publish: ({ idempotencyKey }) => {
        transactions.add(idempotencyKey);
        calls++;
        return calls === 1
          ? Effect.fail(failUpload("Unavailable"))
          : Effect.succeed({ attachmentId: "once" });
      },
    });
    const created = yield* test.uploads.create({ actor, input, idempotencyKey: "lost" });
    yield* test.uploads.content({ actor, uploadId: created.id, body: bytes });
    assert.isTrue(
      Result.isFailure(
        yield* Effect.result(test.uploads.complete({ actor, uploadId: created.id })),
      ),
    );
    assert.strictEqual(
      (yield* test.uploads.status({ actor, uploadId: created.id })).state,
      "publishing",
    );
    const restarted = yield* test.make(yield* test.makeStore());
    assert.strictEqual(
      (yield* restarted.reconcile({ actor, uploadId: created.id })).state,
      "published",
    );
    assert.strictEqual(transactions.size, 1);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "shared KVS ownership serializes concurrent completion through separate service instances",
  () =>
    Effect.gen(function* () {
      let publications = 0;
      const test = yield* setup({
        publish: () =>
          Effect.sync(() => {
            publications++;
            return {};
          }),
      });
      const second = yield* test.make(yield* test.makeStore());
      const created = yield* test.uploads.create({ actor, input, idempotencyKey: "parallel" });
      yield* test.uploads.content({ actor, uploadId: created.id, body: bytes });
      yield* Effect.all(
        [
          test.uploads.complete({ actor, uploadId: created.id }),
          second.complete({ actor, uploadId: created.id }),
        ],
        { concurrency: 2 },
      );
      assert.strictEqual(publications, 1);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "expired and revoked uploads cannot refresh grants and cleanup retains its authority horizon",
  () =>
    Effect.gen(function* () {
      let revoked = false;
      const test = yield* setup({
        authorize: (request) =>
          revoked
            ? Effect.fail(failUpload("Forbidden"))
            : Effect.succeed({
                key: `private/${request.actor.tenantId}/${request.uploadId}`,
                context: {},
                maxBytes: 100,
                allowedContentTypes: ["text/plain"],
                expiresInSeconds: 60,
              }),
      });
      const created = yield* test.uploads.create({ actor, input, idempotencyKey: "expiry" });
      yield* test.uploads.grant({ actor, uploadId: created.id });
      revoked = true;
      assert.isTrue(
        Result.isFailure(yield* Effect.result(test.uploads.grant({ actor, uploadId: created.id }))),
      );
      revoked = false;
      yield* TestClock.adjust("2 minutes");
      const expired = yield* Effect.result(test.uploads.grant({ actor, uploadId: created.id }));
      assert.isTrue(
        Result.isFailure(expired) &&
          expired.failure._tag === "UploadError" &&
          expired.failure.code === "Expired",
      );
      assert.strictEqual(
        (yield* test.uploads.reconcile({ actor, uploadId: created.id })).state,
        "aborted",
      );
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "native handles are protected at rest, redacted on readback and never appear in public status",
  () =>
    Effect.gen(function* () {
      const secret = "https://provider.example/session?token=fixture-secret";
      const test = yield* setup(
        {},
        {
          begin: () => Effect.succeed({ url: secret }),
          grant: (_record, expiresAt) => Effect.succeed({ _tag: "Proxy", expiresAt }),
          abort: () => Effect.void,
        },
      );
      const created = yield* test.uploads.create({ actor, input, idempotencyKey: "secrets" });
      yield* test.uploads.grant({ actor, uploadId: created.id });
      const raw = yield* test.kvs.get(`uploads:upload-intent:${created.id}`);
      assert.notInclude(raw ?? "", secret);
      assert.isTrue(Redacted.isRedacted((yield* test.store.read(created.id))?.remote));
      assert.notInclude(
        JSON.stringify(yield* test.uploads.status({ actor, uploadId: created.id })),
        "fixture-secret",
      );
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("uncertain initiation is not blindly retried", () =>
  Effect.gen(function* () {
    let starts = 0;
    const test = yield* setup(
      {},
      {
        begin: () => {
          starts++;
          return Effect.fail(failUpload("Unavailable"));
        },
        grant: (_record, expiresAt) => Effect.succeed({ _tag: "Proxy", expiresAt }),
        abort: () => Effect.void,
      },
    );
    const created = yield* test.uploads.create({ actor, input, idempotencyKey: "unknown-init" });
    yield* Effect.result(test.uploads.grant({ actor, uploadId: created.id }));
    yield* Effect.result(test.uploads.grant({ actor, uploadId: created.id }));
    assert.strictEqual(starts, 1);
    assert.strictEqual(
      (yield* test.uploads.status({ actor, uploadId: created.id })).state,
      "initiating",
    );
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("rejected inspection remains terminal across completion and grant refresh", () =>
  Effect.gen(function* () {
    const test = yield* setup({ inspect: () => Effect.fail(failUpload("VerificationFailed")) });
    const created = yield* test.uploads.create({
      actor,
      input,
      idempotencyKey: "terminal-inspection",
    });
    yield* test.uploads.content({
      actor,
      uploadId: created.id,
      body: new TextEncoder().encode("hello"),
    });
    yield* Effect.result(test.uploads.complete({ actor, uploadId: created.id }));
    assert.strictEqual(
      (yield* test.uploads.status({ actor, uploadId: created.id })).state,
      "rejected",
    );
    assert.isTrue(
      Result.isFailure(yield* Effect.result(test.uploads.grant({ actor, uploadId: created.id }))),
    );
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect("reconciliation retries interrupted native abort and quota release", () =>
  Effect.gen(function* () {
    let aborts = 0;
    let releases = 0;
    const test = yield* setup(
      {
        quota: {
          reserve: () => Effect.void,
          settle: () => Effect.void,
          release: () => {
            releases++;
            return releases === 1 ? Effect.fail(failUpload("Unavailable")) : Effect.void;
          },
        },
      },
      {
        begin: () => Effect.succeed(null),
        grant: (_record, expiresAt) => Effect.succeed({ _tag: "Proxy", expiresAt }),
        abort: () => {
          aborts++;
          return aborts === 1 ? Effect.fail(failUpload("Unavailable")) : Effect.void;
        },
      },
    );
    const created = yield* test.uploads.create({ actor, input, idempotencyKey: "abort-recovery" });
    const target = { actor, uploadId: created.id };
    assert.isTrue(Result.isFailure(yield* Effect.result(test.uploads.abort(target))));
    assert.strictEqual((yield* test.uploads.status(target)).state, "aborted");
    assert.isTrue(Result.isFailure(yield* Effect.result(test.uploads.reconcile(target))));
    assert.strictEqual((yield* test.uploads.reconcile(target)).state, "aborted");
    assert.strictEqual(aborts, 3);
    assert.strictEqual(releases, 2);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
