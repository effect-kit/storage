import { assert, it } from "@effect/vitest";
import { Context, Effect, Fiber, Layer, Redacted, Result, Stream } from "effect";

import { StoragePublicUrls } from "../../../src/core/capabilities.js";
import { Storage } from "../../../src/core/storage.js";
import { R2WorkersStorage } from "../../../src/providers/r2-workers/index.js";
import * as BrowserStorage from "../../../src/runtime/browser.js";
import { TransferDriver, Transfers, UploadSource } from "../../../src/transfers/index.js";
import { bucket, object } from "../../fixtures/r2-workers/fixture.js";

it.effect("conditional metadata-only reads and null writes map to distinct portable outcomes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* R2WorkersStorage.make({
        bucket: bucket({ get: async () => object(), put: async () => null }),
        storeId: "r2",
      });
      assert.strictEqual(
        (yield* storage.get({ key: "object", condition: { _tag: "IfNoneMatch", etag: "*" } }))._tag,
        "NotModified",
      );
      const failedMatch = yield* storage
        .get({ key: "object", condition: { _tag: "IfMatch", etag: '"stale"' } })
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(failedMatch));
      if (Result.isFailure(failedMatch))
        assert.strictEqual(failedMatch.failure.reason._tag, "PreconditionFailed");
      const failedPut = yield* storage
        .put({ key: "object", body: new Uint8Array(), condition: { _tag: "CreateOnly" } })
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(failedPut));
      if (Result.isFailure(failedPut))
        assert.strictEqual(failedPut.failure.mutationOutcome, "rejected");
      const invalidBody = yield* storage.get({ key: "object" }).pipe(Effect.result);
      assert.isTrue(Result.isFailure(invalidBody));
      if (Result.isFailure(invalidBody))
        assert.strictEqual(invalidBody.failure.reason._tag, "ProviderFailure");
    }),
  ),
);

it.effect("scoped bodies cancel unread responses and preserve range identity", () =>
  Effect.gen(function* () {
    let cancelled = 0;
    const storage = yield* R2WorkersStorage.make({
      bucket: bucket({
        get: async () =>
          object({
            body: new ReadableStream<Uint8Array>({
              cancel: () => {
                cancelled++;
              },
            }),
          }),
      }),
      storeId: "r2",
    });
    yield* Effect.scoped(storage.get({ key: "object" }));
    assert.strictEqual(cancelled, 1);
    const wrongRange = yield* Effect.scoped(
      storage.get({ key: "object", range: { _tag: "From", start: 1 } }),
    ).pipe(Effect.result);
    assert.isTrue(Result.isFailure(wrongRange));
    if (Result.isFailure(wrongRange))
      assert.strictEqual(wrongRange.failure.reason._tag, "ProviderFailure");
    assert.strictEqual(cancelled, 2);
  }),
);

it.effect("unknown-length staging is bounded and preserves application source failures", () =>
  Effect.gen(function* () {
    let writes = 0;
    const storage = yield* R2WorkersStorage.make({
      bucket: bucket({
        put: async () => {
          writes++;
          return object();
        },
      }),
      storeId: "r2",
      maxUnknownLengthBytes: 2,
    });
    const tooLarge = yield* storage
      .put({ key: "object", body: Stream.succeed(new Uint8Array(3)) })
      .pipe(Effect.result);
    assert.isTrue(Result.isFailure(tooLarge));
    if (Result.isFailure(tooLarge))
      assert.strictEqual(tooLarge.failure.reason._tag, "LimitExceeded");
    const error = { _tag: "CallerSource" } as const;
    const failed = yield* storage
      .put({ key: "object", body: Stream.fail(error) })
      .pipe(Effect.result);
    assert.isTrue(Result.isFailure(failed));
    if (Result.isFailure(failed)) assert.strictEqual(failed.failure, error);
    assert.strictEqual(writes, 0);
  }),
);

it.effect("native error mapping retains safe categories without raw provider messages", () =>
  Effect.gen(function* () {
    for (const [code, reason] of [
      [10039, "RangeNotSatisfiable"],
      [10007, "NotFound"],
      [10031, "PreconditionFailed"],
      [10001, "ProviderFailure"],
      [10058, "Throttled"],
      [10043, "ProviderFailure"],
      [10003, "AccessDenied"],
      [10069, "AccessDenied"],
      [10054, "TransportFailure"],
    ] as const) {
      const storage = yield* R2WorkersStorage.make({
        bucket: bucket({
          head: async () => {
            throw new Error(`private-secret (${code})`);
          },
        }),
        storeId: "r2",
      });
      const result = yield* storage.head({ key: "object" }).pipe(Effect.result);
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, reason);
        assert.notInclude(JSON.stringify(result.failure), "private-secret");
      }
    }
  }),
);

it.effect(
  "provider driver forbids conditional multipart before source opening or remote initiation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let opened = false;
        const context = yield* Layer.build(
          Transfers.layer({ maxBytes: 16 * 1024 * 1024, memoryBudgetBytes: 64 * 1024 * 1024 }).pipe(
            Layer.provideMerge(R2WorkersStorage.layer({ bucket: bucket(), storeId: "r2" })),
          ),
        );
        const driver = Context.get(context, TransferDriver);
        assert.strictEqual(driver.storage, Context.get(context, Storage));
        assert.isUndefined(driver.handles);
        const transfers = Context.get(context, Transfers);
        const result = yield* transfers
          .upload({
            key: "object",
            strategy: "multipart",
            condition: { _tag: "CreateOnly" },
            source: UploadSource.replayable(() => {
              opened = true;
              return Stream.empty;
            }, 0),
          })
          .pipe(Effect.result);
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) {
          assert.strictEqual(result.failure.reason._tag, "Unsupported");
          assert.strictEqual(result.failure.mutationOutcome, "not-started");
        }
        assert.isFalse(opened);
        if (!driver.multipart) return yield* Effect.die("missing multipart");
        const inventory = yield* driver.multipart
          .listParts({
            upload: { ref: { storeId: "r2", key: "object" }, uploadId: Redacted.make("opaque") },
          })
          .pipe(Effect.result);
        assert.isTrue(Result.isFailure(inventory));
      }),
    ).pipe(Effect.provide(BrowserStorage.layer)),
);

it.effect("cancelling a binding promise does not fabricate remote mutation cancellation", () =>
  Effect.gen(function* () {
    let finish: (() => void) | undefined;
    let committed = false;
    const storage = yield* R2WorkersStorage.make({
      bucket: bucket({
        put: () =>
          new Promise((accept) => {
            finish = () => {
              committed = true;
              accept(object());
            };
          }),
      }),
      storeId: "r2",
    });
    const fiber = yield* storage
      .put({ key: "object", body: new Uint8Array(3) })
      .pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    yield* Fiber.interrupt(fiber);
    finish?.();
    assert.isTrue(committed);
  }),
);

it.effect("public URLs require configured authority and reject unsafe reference resolution", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Layer.build(
        R2WorkersStorage.publicUrls("https://assets.example.test/files/").pipe(
          Layer.provide(R2WorkersStorage.layer({ bucket: bucket(), storeId: "r2" })),
        ),
      );
      const urls = Context.get(context, StoragePublicUrls);
      assert.strictEqual(
        yield* urls.url({ storeId: "r2", key: "hello world/a%2Fb" }),
        "https://assets.example.test/files/hello%20world/a%252Fb",
      );
      assert.isTrue(
        Result.isFailure(yield* urls.url({ storeId: "r2", key: "../escape" }).pipe(Effect.result)),
      );
    }),
  ),
);

it.effect("a committed write with an invalid receipt retains a succeeded mutation outcome", () =>
  Effect.gen(function* () {
    const storage = yield* R2WorkersStorage.make({
      bucket: bucket({ put: async () => object({ size: -1 }) }),
      storeId: "r2",
    });
    const result = yield* storage
      .put({ key: "object", body: new Uint8Array(3) })
      .pipe(Effect.result);
    assert.isTrue(Result.isFailure(result));
    if (Result.isFailure(result)) assert.strictEqual(result.failure.mutationOutcome, "succeeded");
  }),
);

it.effect("a delayed read acquisition joins before cancelling its newly owned body", () =>
  Effect.gen(function* () {
    let finish: ((response: ReturnType<typeof object>) => void) | undefined;
    let cancelled = 0;
    const storage = yield* R2WorkersStorage.make({
      bucket: bucket({
        get: () =>
          new Promise((accept) => {
            finish = accept;
          }),
      }),
      storeId: "r2",
    });
    const read = yield* Effect.scoped(storage.get({ key: "object" })).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    const interrupted = yield* Fiber.interrupt(read).pipe(Effect.forkChild);
    yield* Effect.yieldNow;
    assert.isUndefined(interrupted.pollUnsafe());
    finish?.(
      object({
        body: new ReadableStream<Uint8Array>({
          cancel: () => {
            cancelled++;
          },
        }),
      }),
    );
    yield* Fiber.join(interrupted);
    assert.strictEqual(cancelled, 1);
  }),
);
