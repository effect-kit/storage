import { Context, DateTime, Effect, Layer, Schema } from "effect";

import {
  DeleteObjectCommand,
  GetObjectLegalHoldCommand,
  GetObjectRetentionCommand,
  PutObjectLegalHoldCommand,
  PutObjectRetentionCommand,
} from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { VersionRef } from "../lifecycle.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { lifecycleFeature, lifecycleTarget } from "./lifecycle-validation.js";
import { request } from "./transport.js";

export const GovernanceBypass = Schema.Struct({
  intent: Schema.Literal("bypass-governance"),
  reason: Schema.NonEmptyString,
});
export const RetentionPolicy = Schema.Struct({
  mode: Schema.Literals(["governance", "compliance"]),
  retainUntil: Schema.DateTimeUtc,
});
const Target = Schema.Struct({ ref: VersionRef });
const SetRetention = Schema.Struct({
  ...Target.fields,
  retention: RetentionPolicy,
  bypassGovernance: Schema.optional(GovernanceBypass),
});
const SetHold = Schema.Struct({
  ...Target.fields,
  status: Schema.Literals(["on", "off"]),
  releaseIntent: Schema.optional(Schema.Literal("release-object-hold")),
});
const DeleteProtected = Schema.Struct({ ...Target.fields, bypassGovernance: GovernanceBypass });

const make = (storage: S3StorageService) => {
  const selected = Effect.fnUntraced(function* (ref: VersionRef, operation: string) {
    yield* lifecycleFeature(storage, "protection");
    return yield* lifecycleTarget(storage, ref, operation, true);
  });
  return {
    getRetention: Effect.fnUntraced(function* (input: typeof Target.Type) {
      const value = yield* decodeRequest(Target, input, "getRetention", storage.provider);
      const target = yield* selected(value.ref, "getRetention");
      const output = yield* request(storage, "getRetention", false, (signal) =>
        storage.client.send(new GetObjectRetentionCommand(target), { abortSignal: signal }),
      );
      if (!output.Retention?.Mode && !output.Retention?.RetainUntilDate)
        return { _tag: "None" as const };
      if (
        !output.Retention?.Mode ||
        !["GOVERNANCE", "COMPLIANCE"].includes(output.Retention.Mode) ||
        !output.Retention.RetainUntilDate ||
        !Number.isFinite(output.Retention.RetainUntilDate.getTime())
      )
        return yield* storageError("getRetention", storage.provider, {
          _tag: "ProviderFailure",
          code: "InvalidRetention",
        });
      return {
        _tag: "Retention" as const,
        mode:
          output.Retention.Mode === "GOVERNANCE"
            ? ("governance" as const)
            : ("compliance" as const),
        retainUntil: DateTime.makeUnsafe(output.Retention.RetainUntilDate),
      };
    }),
    setRetention: Effect.fnUntraced(function* (input: typeof SetRetention.Type) {
      const value = yield* decodeRequest(
        SetRetention,
        input,
        "setRetention",
        storage.provider,
        true,
      );
      const target = yield* selected(value.ref, "setRetention");
      const now = yield* DateTime.now;
      if (DateTime.toEpochMillis(value.retention.retainUntil) <= DateTime.toEpochMillis(now))
        return yield* storageError(
          "setRetention",
          storage.provider,
          { _tag: "InvalidInput", field: "retainUntil" },
          "not-started",
        );
      yield* request(storage, "setRetention", true, (signal) =>
        storage.client.send(
          new PutObjectRetentionCommand({
            ...target,
            Retention: {
              Mode: value.retention.mode === "governance" ? "GOVERNANCE" : "COMPLIANCE",
              RetainUntilDate: DateTime.toDateUtc(value.retention.retainUntil),
            },
            ...(value.bypassGovernance ? { BypassGovernanceRetention: true } : {}),
          }),
          { abortSignal: signal },
        ),
      );
      return { _tag: "Accepted" as const };
    }),
    getLegalHold: Effect.fnUntraced(function* (input: typeof Target.Type) {
      const value = yield* decodeRequest(Target, input, "getLegalHold", storage.provider);
      const target = yield* selected(value.ref, "getLegalHold");
      const output = yield* request(storage, "getLegalHold", false, (signal) =>
        storage.client.send(new GetObjectLegalHoldCommand(target), { abortSignal: signal }),
      );
      return {
        status:
          output.LegalHold?.Status === "ON"
            ? ("on" as const)
            : output.LegalHold?.Status === "OFF"
              ? ("off" as const)
              : ("unreported" as const),
      };
    }),
    setLegalHold: Effect.fnUntraced(function* (input: typeof SetHold.Type) {
      const value = yield* decodeRequest(SetHold, input, "setLegalHold", storage.provider, true);
      if (value.status === "off" && !value.releaseIntent)
        return yield* storageError(
          "setLegalHold",
          storage.provider,
          { _tag: "InvalidInput", field: "releaseIntent" },
          "not-started",
        );
      const target = yield* selected(value.ref, "setLegalHold");
      yield* request(storage, "setLegalHold", true, (signal) =>
        storage.client.send(
          new PutObjectLegalHoldCommand({
            ...target,
            LegalHold: { Status: value.status === "on" ? "ON" : "OFF" },
          }),
          { abortSignal: signal },
        ),
      );
      return { _tag: "Accepted" as const };
    }),
    deleteVersion: Effect.fnUntraced(function* (input: typeof DeleteProtected.Type) {
      const value = yield* decodeRequest(
        DeleteProtected,
        input,
        "deleteProtectedVersion",
        storage.provider,
        true,
      );
      const target = yield* selected(value.ref, "deleteProtectedVersion");
      yield* request(storage, "deleteProtectedVersion", true, (signal) =>
        storage.client.send(
          new DeleteObjectCommand({ ...target, BypassGovernanceRetention: true }),
          { abortSignal: signal },
        ),
      );
    }),
  };
};
export type S3ObjectLockService = ReturnType<typeof make>;
export class S3ObjectLock extends Context.Service<S3ObjectLock, S3ObjectLockService>()(
  "@effect-kit/storage/s3/S3ObjectLock",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(S3ObjectLock, Effect.map(S3Storage, make));
}
