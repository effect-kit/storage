import { Effect, Redacted, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import type { ObjectRef } from "../../core/object.js";
import type { WriteCondition } from "../../core/requests.js";
import { lease } from "./config.js";
import type { AzureConnection } from "./config.js";
import { condition, metadata, target } from "./mapping.js";
import { request } from "./transport.js";
import { validateKey } from "./validation.js";

export function nativeOperations(connection: AzureConnection) {
  return {
    beginCopy: Effect.fnUntraced(function* (input: {
      readonly sourceUrl: Redacted.Redacted<string>;
      readonly destinationKey: string;
      readonly sourceIfMatch?: string;
      readonly condition?: WriteCondition;
    }) {
      if (connection.config.encryption)
        return yield* storageError(
          "beginCopy",
          "azure",
          { _tag: "Unsupported", capability: "encrypted-async-copy" },
          "not-started",
        );
      const url = URL.parse(Redacted.value(input.sourceUrl));
      if (!url || url.protocol !== "https:" || url.username || url.password || url.hash)
        return yield* storageError(
          "beginCopy",
          "azure",
          { _tag: "InvalidInput", field: "sourceUrl" },
          "not-started",
        );
      yield* validateKey(input.destinationKey, "beginCopy", true);
      const client = connection.client.getBlockBlobClient(input.destinationKey);
      const poller = yield* request(connection, "beginCopy", true, (signal) =>
        client.beginCopyFromURL(url.href, {
          abortSignal: signal,
          conditions: { ...condition(input.condition), ...lease(connection.config) },
          ...(input.sourceIfMatch ? { sourceConditions: { ifMatch: input.sourceIfMatch } } : {}),
        }),
      );
      poller.stopPolling();
      // Pending copies have no getResult(); the public operation state retains their copy ID.
      const state = yield* Schema.decodeUnknownEffect(
        Schema.Struct({ copyId: Schema.NonEmptyString }),
      )(poller.getOperationState()).pipe(
        Effect.mapError(() =>
          storageError("beginCopy", "azure", { _tag: "ProviderFailure" }, "unknown"),
        ),
      );
      return {
        _tag: "Started" as const,
        copyId: state.copyId,
        ref: { storeId: connection.config.storeId, key: input.destinationKey },
      };
    }),
    pollCopy: Effect.fnUntraced(function* (input: {
      readonly ref: ObjectRef;
      readonly copyId: string;
    }) {
      const address = yield* target(connection, { ref: input.ref }, "pollCopy");
      const result = yield* request(connection, "pollCopy", false, (signal) =>
        address.client.getProperties({ abortSignal: signal, conditions: lease(connection.config) }),
      );
      if (result.copyId !== input.copyId)
        return yield* storageError("pollCopy", "azure", { _tag: "PreconditionFailed" });
      return result.copyStatus === "success"
        ? {
            _tag: "Completed" as const,
            metadata: metadata(connection, input.ref.key, result, input.ref),
          }
        : result.copyStatus === "failed" || result.copyStatus === "aborted"
          ? { _tag: "Failed" as const, status: result.copyStatus }
          : { _tag: "Pending" as const, status: result.copyStatus ?? "unknown" };
    }),
    cancelCopy: Effect.fnUntraced(function* (input: {
      readonly ref: ObjectRef;
      readonly copyId: string;
    }) {
      const address = yield* target(connection, { ref: input.ref }, "cancelCopy");
      yield* request(connection, "cancelCopy", true, (signal) =>
        address.client.abortCopyFromURL(input.copyId, {
          abortSignal: signal,
          conditions: lease(connection.config),
        }),
      );
    }),
    acquireLease: Effect.fnUntraced(function* (input: {
      readonly ref: ObjectRef;
      readonly durationSeconds: number;
      readonly proposedId?: Redacted.Redacted<string>;
    }) {
      if (
        input.durationSeconds !== -1 &&
        (!Number.isInteger(input.durationSeconds) ||
          input.durationSeconds < 15 ||
          input.durationSeconds > 60)
      )
        return yield* storageError(
          "acquireLease",
          "azure",
          { _tag: "InvalidInput", field: "durationSeconds" },
          "not-started",
        );
      const address = yield* target(connection, { ref: input.ref }, "acquireLease");
      const result = yield* request(connection, "acquireLease", true, (signal) =>
        address.client
          .getBlobLeaseClient(input.proposedId ? Redacted.value(input.proposedId) : undefined)
          .acquireLease(input.durationSeconds, { abortSignal: signal }),
      );
      if (!result.leaseId)
        return yield* storageError("acquireLease", "azure", { _tag: "ProviderFailure" }, "unknown");
      return Redacted.make(result.leaseId);
    }),
    renewLease: Effect.fnUntraced(function* (input: {
      readonly ref: ObjectRef;
      readonly leaseId: Redacted.Redacted<string>;
    }) {
      const address = yield* target(connection, { ref: input.ref }, "renewLease");
      yield* request(connection, "renewLease", true, (signal) =>
        address.client
          .getBlobLeaseClient(Redacted.value(input.leaseId))
          .renewLease({ abortSignal: signal }),
      );
    }),
    releaseLease: Effect.fnUntraced(function* (input: {
      readonly ref: ObjectRef;
      readonly leaseId: Redacted.Redacted<string>;
    }) {
      const address = yield* target(connection, { ref: input.ref }, "releaseLease");
      yield* request(connection, "releaseLease", true, (signal) =>
        address.client
          .getBlobLeaseClient(Redacted.value(input.leaseId))
          .releaseLease({ abortSignal: signal }),
      );
    }),
  };
}
