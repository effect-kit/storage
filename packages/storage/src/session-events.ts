import { Effect } from "effect";

import { storageError } from "./errors.js";
import type { StorageError } from "./errors.js";
import type { StorageEvent } from "./events.js";
import type { ObjectMetadata, ObjectRef } from "./object.js";
import { UploadSessions } from "./sessions.js";

/** Resolve from an application-owned index. An event is a hint; reconciliation verifies remotely. */
export const sessionEventDelivery = Effect.fnUntraced(function* <E, R>(options: {
  readonly resolve: (
    event: StorageEvent,
  ) => Effect.Effect<{ readonly sessionId: string; readonly target: ObjectRef } | undefined, E, R>;
  readonly outbox: (input: {
    readonly id: string;
    readonly kind: "completed" | "aborted";
    readonly sessionId: string;
    readonly metadata?: ObjectMetadata | undefined;
  }) => Effect.Effect<void, E, R>;
}) {
  const sessions = yield* UploadSessions;
  return Effect.fnUntraced(function* (input: {
    readonly event: StorageEvent;
    readonly idempotencyKey: string;
  }): Effect.fn.Return<void, E | StorageError, R> {
    const binding = yield* options.resolve(input.event);
    if (!binding) return;
    if (
      binding.target.storeId !== input.event.ref.storeId ||
      binding.target.key !== input.event.ref.key ||
      (binding.target.version &&
        (binding.target.version._tag !== input.event.ref.version?._tag ||
          binding.target.version.id !== input.event.ref.version.id))
    )
      return yield* storageError("reconcileEvent", "sessions", {
        _tag: "InvalidInput",
        field: "event-session-binding",
      });
    yield* sessions.reconcile(binding.sessionId);
    yield* sessions.deliverEvents(binding.sessionId, options.outbox);
  });
});
