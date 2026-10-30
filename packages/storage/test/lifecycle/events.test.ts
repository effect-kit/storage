import { assert, it } from "@effect/vitest";
import { DateTime, Effect, Result } from "effect";
import { KeyValueStore } from "effect/persistence";

import { AzureEvents } from "../../src/azure/events.js";
import { storageError } from "../../src/errors.js";
import { StorageEventInbox, makeEventProcessor } from "../../src/events.js";
import type { StorageEvent } from "../../src/events.js";
import { GcsEvents } from "../../src/gcs/events.js";
import { S3Events } from "../../src/s3/events.js";

const event = (id: string, sequencer?: string): StorageEvent => ({
  provider: "s3",
  kind: "object-created",
  ref: { storeId: "store", key: "file" },
  occurredAt: DateTime.makeUnsafe("2026-10-30T00:00:00Z"),
  id,
  nativeType: "ObjectCreated:Put",
  ...(sequencer ? { sequencer } : {}),
});

it.effect(
  "GCS noncurrent versions remain distinct from deleted objects and S3 restore events accept transport IDs",
  () =>
    Effect.gen(function* () {
      const archived = yield* GcsEvents.decode(
        {
          messageId: "delivery",
          publishTime: "2026-10-30T00:00:00Z",
          attributes: {
            bucketId: "owned",
            objectId: "file",
            objectGeneration: "9007199254740993",
            eventType: "OBJECT_ARCHIVE",
          },
        },
        { bucket: "owned", storeId: "store" },
      );
      assert.strictEqual(archived[0]?.kind, "version-archived");
      const restored = yield* S3Events.decode(
        {
          Records: [
            {
              eventVersion: "2.3",
              eventTime: "2026-10-30T00:00:00Z",
              eventName: "ObjectRestore:Completed",
              s3: { bucket: { name: "owned" }, object: { key: "file" } },
            },
          ],
        },
        { bucket: "owned", storeId: "store", messageId: "sns-message-id" },
      );
      assert.strictEqual(restored[0]?.kind, "restore-completed");
      assert.strictEqual(restored[0]?.id, "sns-message-id:0");
    }),
);

it.effect(
  "S3 events decode form-encoded keys once, preserve versions and reject foreign buckets",
  () =>
    Effect.gen(function* () {
      const payload = {
        Records: [
          {
            eventVersion: "2.1",
            awsRegion: "us-east-1",
            responseElements: {},
            eventTime: "2026-10-30T00:00:00Z",
            eventName: "ObjectCreated:Put",
            s3: {
              bucket: { name: "owned" },
              object: { key: "a%252Fb+c", versionId: "9007199254740993", sequencer: "A" },
            },
          },
        ],
      };
      const events = yield* S3Events.decode(payload, { bucket: "owned", storeId: "store" });
      assert.strictEqual(events[0]?.ref.key, "a%2Fb c");
      assert.strictEqual(events[0]?.ref.version?.id, "9007199254740993");
      assert.isDefined(events[0]?.id);
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(S3Events.decode(payload, { bucket: "foreign", storeId: "store" })),
        ),
      );
      const malformed = {
        ...payload,
        Records: [
          { ...payload.Records[0], s3: { bucket: { name: "owned" }, object: { key: "%GG" } } },
        ],
      };
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(S3Events.decode(malformed, { bucket: "owned", storeId: "store" })),
        ),
      );
    }),
);

it.effect(
  "Azure event URLs remain inside the configured container and GCS object IDs stay opaque",
  () =>
    Effect.gen(function* () {
      const payload = [
        {
          id: "delivery",
          eventTime: "2026-10-30T00:00:00Z",
          eventType: "Microsoft.Storage.BlobCreated",
          topic: "native-topic",
          subject: "native-subject",
          data: { url: "https://account.blob.core.windows.net/owned/a%252Fb", sequencer: "0A" },
        },
      ];
      const events = yield* AzureEvents.decode(payload, {
        containerUrl: "https://account.blob.core.windows.net/owned",
        storeId: "store",
      });
      assert.strictEqual(events[0]?.ref.key, "a%2Fb");
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(
            AzureEvents.decode(payload, {
              containerUrl: "https://account.blob.core.windows.net/other",
              storeId: "store",
            }),
          ),
        ),
      );
      const gcs = yield* GcsEvents.decode(
        {
          messageId: "pubsub-id",
          data: "base64-provider-data",
          publishTime: "2026-10-30T00:00:00Z",
          attributes: {
            bucketId: "owned",
            objectId: "a%2Fb+c",
            objectGeneration: "9007199254740993",
            eventType: "OBJECT_FINALIZE",
          },
        },
        { bucket: "owned", storeId: "store" },
      );
      assert.strictEqual(gcs[0]?.ref.key, "a%2Fb+c");
      assert.strictEqual(gcs[0]?.ref.version?.id, "9007199254740993");
    }),
);

it.effect("exclusive inbox instances serialize duplicates and preserve native per-key order", () =>
  Effect.gen(function* () {
    const first = yield* StorageEventInbox.make({ namespace: "owned", ownership: "exclusive" });
    const second = yield* StorageEventInbox.make({ namespace: "owned", ownership: "exclusive" });
    let calls = 0;
    const deliver = () =>
      Effect.sync(() => {
        calls++;
      });
    const statuses = yield* Effect.all(
      [first.process(event("one", "1"), deliver), second.process(event("one", "1"), deliver)],
      { concurrency: 2 },
    );
    assert.deepStrictEqual(statuses.toSorted(), ["delivered", "duplicate"]);
    assert.strictEqual(calls, 1);
    assert.strictEqual(yield* first.process(event("three", "003"), deliver), "delivered");
    assert.strictEqual(yield* first.process(event("two", "02"), deliver), "out-of-order");
    assert.strictEqual(calls, 2);
  }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "failed application delivery is not acknowledged and retries carry a stable idempotency key",
  () =>
    Effect.gen(function* () {
      const inbox = yield* StorageEventInbox.make({ namespace: "owned", ownership: "exclusive" });
      const keys: string[] = [];
      const failure = yield* Effect.result(
        inbox.process(event("retry"), ({ idempotencyKey }) => {
          keys.push(idempotencyKey);
          return Effect.fail("application-unavailable");
        }),
      );
      assert.isTrue(Result.isFailure(failure));
      assert.strictEqual(
        yield* inbox.process(event("retry"), ({ idempotencyKey }) =>
          Effect.sync(() => {
            keys.push(idempotencyKey);
          }),
        ),
        "delivered",
      );
      assert.strictEqual(keys[0], keys[1]);
      assert.strictEqual(
        yield* inbox.process(event("retry"), () => Effect.die("must not run")),
        "duplicate",
      );
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);

it.effect(
  "authentication runs before inbox processing and forged deliveries cannot invoke application code",
  () =>
    Effect.gen(function* () {
      const inbox = yield* StorageEventInbox.make({ namespace: "owned", ownership: "exclusive" });
      let delivered = false;
      const process = yield* makeEventProcessor({
        authenticate: (_payload: unknown) =>
          Effect.fail(storageError("authenticate", "test", { _tag: "AccessDenied" })),
        deliver: () =>
          Effect.sync(() => {
            delivered = true;
          }),
      }).pipe(Effect.provideService(StorageEventInbox, inbox));
      const result = yield* Effect.result(process({ forged: true }));
      assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "AccessDenied");
      assert.isFalse(delivered);
    }).pipe(Effect.provide(KeyValueStore.layerMemory)),
);
