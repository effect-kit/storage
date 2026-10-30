import { Context, DateTime, Duration, Effect, Layer, Schema } from "effect";

import { HeadObjectCommand, RestoreObjectCommand } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import type { ArchiveService, RestoreStatus } from "../lifecycle.js";
import { ObjectRef } from "../object.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { readEncryption } from "./config.js";
import { lifecycleFeature, lifecycleTarget } from "./lifecycle-validation.js";
import { request } from "./transport.js";

const Options = Schema.Union([
  Schema.Struct({
    days: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 365 })),
    tier: Schema.Literals(["Standard", "Bulk", "Expedited"]),
  }),
  Schema.Struct({
    storage: Schema.Literal("intelligent-tiering"),
    tier: Schema.Literals(["Standard", "Bulk", "Expedited"]),
  }),
]);
export type S3RestoreOptions = typeof Options.Type;
export class S3Archive extends Context.Service<S3Archive, ArchiveService<S3RestoreOptions>>()(
  "@effect-kit/storage/s3/S3Archive",
) {
  static make(storage: S3StorageService): ArchiveService<S3RestoreOptions> {
    const restoreStatus = Effect.fnUntraced(function* (input: {
      readonly ref: ObjectRef;
    }): Effect.fn.Return<RestoreStatus, ReturnType<typeof storageError>> {
      const { ref } = yield* decodeRequest(
        Schema.Struct({ ref: ObjectRef }),
        input,
        "restoreStatus",
        storage.provider,
      );
      yield* lifecycleFeature(storage, "archive");
      const target = yield* lifecycleTarget(storage, ref, "restoreStatus");
      const result = yield* request(storage, "restoreStatus", false, (signal) =>
        storage.client.send(
          new HeadObjectCommand({ ...target, ...readEncryption(storage.config.encryption) }),
          { abortSignal: signal },
        ),
      );
      if (result.Restore?.includes('ongoing-request="true"')) return { _tag: "Pending" };
      const expiry = /expiry-date="([^"]+)"/u.exec(result.Restore ?? "")?.[1];
      if (expiry) {
        const parsed = Date.parse(expiry);
        if (!Number.isFinite(parsed))
          return yield* storageError("restoreStatus", storage.provider, {
            _tag: "ProviderFailure",
            code: "InvalidRestoreDate",
          });
        return { _tag: "Ready", availableUntil: DateTime.makeUnsafe(parsed) };
      }
      return {
        _tag:
          ["GLACIER", "DEEP_ARCHIVE"].includes(result.StorageClass ?? "") || result.ArchiveStatus
            ? "Archived"
            : "Ready",
      };
    });
    return {
      restoreStatus,
      requestRestore: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(
          Schema.Struct({ ref: ObjectRef, options: Options }),
          input,
          "requestRestore",
          storage.provider,
          true,
        );
        yield* lifecycleFeature(storage, "archive");
        if (
          storage.provider === "tigris" &&
          (!("days" in value.options) || value.options.tier !== "Standard")
        )
          return yield* storageError(
            "requestRestore",
            "tigris",
            { _tag: "Unsupported", capability: "aws-restore-tier-options" },
            "not-started",
          );
        const target = yield* lifecycleTarget(storage, value.ref, "requestRestore");
        yield* request(storage, "requestRestore", true, (signal) =>
          storage.client.send(
            new RestoreObjectCommand({
              ...target,
              RestoreRequest:
                storage.provider === "tigris" && "days" in value.options
                  ? { Days: value.options.days }
                  : "days" in value.options
                    ? {
                        Days: value.options.days,
                        GlacierJobParameters: { Tier: value.options.tier },
                      }
                    : { Tier: value.options.tier },
            }),
            { abortSignal: signal },
          ),
        );
        return { _tag: "Pending" as const };
      }),
      awaitReady: Effect.fnUntraced(function* (input) {
        const polling = yield* Effect.try({
          try: () => ({
            timeout: Duration.fromInputUnsafe(input.timeout),
            interval: Duration.fromInputUnsafe(input.pollInterval),
          }),
          catch: () =>
            storageError("awaitRestore", storage.provider, {
              _tag: "InvalidInput",
              field: "polling",
            }),
        });
        if (
          !Duration.isFinite(polling.timeout) ||
          !Duration.isFinite(polling.interval) ||
          Duration.toMillis(polling.timeout) <= 0 ||
          Duration.toMillis(polling.interval) <= 0
        )
          return yield* storageError("awaitRestore", storage.provider, {
            _tag: "InvalidInput",
            field: "polling",
          });
        return yield* Effect.gen(function* () {
          while (true) {
            const state = yield* restoreStatus({ ref: input.ref });
            if (state._tag === "Ready") return state;
            yield* Effect.sleep(input.pollInterval);
          }
        }).pipe(
          Effect.timeoutOrElse({
            duration: input.timeout,
            orElse: () =>
              Effect.fail(
                storageError("awaitRestore", storage.provider, {
                  _tag: "TransportFailure",
                  code: "DeadlineExceeded",
                }),
              ),
          }),
        );
      }),
    };
  }
  static readonly layer = Layer.effect(S3Archive, Effect.map(S3Storage, S3Archive.make));
}

export { S3StorageTier } from "./tier.js";
