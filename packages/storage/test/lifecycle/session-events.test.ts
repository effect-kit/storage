import { assert, it } from "@effect/vitest";
import { DateTime, Effect, Result } from "effect";

import type { StorageEvent } from "../../src/events.js";
import { sessionEventDelivery } from "../../src/session-events.js";
import { UploadSessions } from "../../src/sessions.js";
import type { SessionsService } from "../../src/sessions.js";

const event: StorageEvent = {
  provider: "s3",
  kind: "object-created",
  ref: { storeId: "owned", key: "file" },
  occurredAt: DateTime.makeUnsafe("2026-10-30T00:00:00Z"),
  id: "event",
  nativeType: "ObjectCreated:Put",
};

const unused = () => Effect.die(new Error("Unexpected session operation"));
const untouched = {
  create: unused,
  resume: unused,
  complete: unused,
  status: unused,
  abort: unused,
  forget: unused,
  createDownload: unused,
  resumeDownload: unused,
  abortDownload: unused,
  createBatch: unused,
  processBatch: unused,
};

it.effect(
  "session event delivery rejects mismatched application bindings before reconciliation",
  () =>
    Effect.gen(function* () {
      let reconciled = false;
      const sessions: SessionsService = {
        ...untouched,
        reconcile: () =>
          Effect.sync(() => {
            reconciled = true;
            return {
              id: "session",
              kind: "upload" as const,
              state: "completed",
              revision: 1,
              expired: false,
            };
          }),
        deliverEvents: () => Effect.void,
      };
      const deliver = yield* sessionEventDelivery({
        resolve: () =>
          Effect.succeed({
            sessionId: "session",
            target: { storeId: "other-tenant", key: "file" },
          }),
        outbox: () => Effect.void,
      }).pipe(Effect.provideService(UploadSessions, sessions));
      const result = yield* Effect.result(deliver({ event, idempotencyKey: "delivery" }));
      assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "InvalidInput");
      assert.isFalse(reconciled);
    }),
);

it.effect(
  "session events reconcile remotely before durable outbox delivery and preserve callback failures",
  () =>
    Effect.gen(function* () {
      const order: string[] = [];
      const sessions: SessionsService = {
        ...untouched,
        reconcile: () =>
          Effect.sync(() => {
            order.push("reconcile");
            return {
              id: "session",
              kind: "upload" as const,
              state: "completed",
              revision: 1,
              expired: false,
            };
          }),
        deliverEvents: (_id, callback) =>
          callback({ id: "session:completed", kind: "completed", sessionId: "session" }),
      };
      const deliver = yield* sessionEventDelivery({
        resolve: () => Effect.succeed({ sessionId: "session", target: event.ref }),
        outbox: ({ id }) => {
          order.push(id);
          return Effect.fail("outbox-unavailable");
        },
      }).pipe(Effect.provideService(UploadSessions, sessions));
      const result = yield* Effect.result(deliver({ event, idempotencyKey: "delivery" }));
      assert.isTrue(Result.isFailure(result) && result.failure === "outbox-unavailable");
      assert.deepStrictEqual(order, ["reconcile", "session:completed"]);
    }),
);
