import { Context, DateTime, Effect, Layer, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { VersionRef } from "../../lifecycle/index.js";
import { GcsStorage } from "./index.js";
import type { GcsStorageService } from "./index.js";
import { target } from "./mapping.js";
import { jsonRequest, decodeJson } from "./transport.js";

const Request = Schema.Struct({
  ref: VersionRef,
  temporaryHold: Schema.optional(Schema.Boolean),
  eventBasedHold: Schema.optional(Schema.Boolean),
  releaseIntent: Schema.optional(Schema.Literal("release-object-hold")),
});
const Retention = Schema.Struct({
  ref: VersionRef,
  mode: Schema.Literals(["Locked", "Unlocked"]),
  retainUntil: Schema.DateTimeUtc,
  intent: Schema.Literal("change-object-retention"),
  override: Schema.optional(Schema.Literal("override-unlocked-retention")),
});
const make = (storage: GcsStorageService) => ({
  setRetention: Effect.fnUntraced(function* (input: typeof Retention.Type) {
    const value = yield* decodeRequest(Retention, input, "setRetention", "gcs", true);
    const address = yield* target(storage, { ref: value.ref }, "setRetention");
    if (
      value.ref.version._tag !== "GcsGeneration" ||
      !value.ref.version.metageneration ||
      DateTime.toEpochMillis(value.retainUntil) <= DateTime.toEpochMillis(yield* DateTime.now)
    )
      return yield* storageError(
        "setRetention",
        "gcs",
        { _tag: "InvalidInput", field: "metageneration-or-retainUntil" },
        "not-started",
      );
    return (yield* jsonRequest(storage, "setRetention", true, {
      uri: address.uri,
      method: "PATCH",
      qs: {
        ...address.qs,
        ifMetagenerationMatch: value.ref.version.metageneration,
        ...(value.override ? { overrideUnlockedRetention: "true" } : {}),
      },
      json: {
        retention: { mode: value.mode, retainUntilTime: DateTime.formatIso(value.retainUntil) },
      },
    })).body;
  }),
  inspect: Effect.fnUntraced(function* (input: { readonly ref: VersionRef }) {
    const value = yield* decodeRequest(
      Schema.Struct({ ref: VersionRef }),
      input,
      "inspectProtection",
      "gcs",
    );
    const address = yield* target(storage, { ref: value.ref }, "inspectProtection");
    const output = yield* jsonRequest(storage, "inspectProtection", false, {
      uri: address.uri,
      qs: address.qs,
      json: true,
    });
    return yield* decodeJson(
      Schema.Struct({
        temporaryHold: Schema.optional(Schema.Boolean),
        eventBasedHold: Schema.optional(Schema.Boolean),
        retentionExpirationTime: Schema.optional(Schema.String),
        retention: Schema.optional(
          Schema.Struct({ mode: Schema.String, retainUntilTime: Schema.String }),
        ),
        kmsKeyName: Schema.optional(Schema.String),
        customerEncryption: Schema.optional(
          Schema.Struct({
            encryptionAlgorithm: Schema.optional(Schema.String),
            keySha256: Schema.optional(Schema.String),
          }),
        ),
      }),
      output.body,
      "inspectProtection",
    );
  }),
  setHolds: Effect.fnUntraced(function* (input: typeof Request.Type) {
    const value = yield* decodeRequest(Request, input, "setHolds", "gcs", true);
    if ((value.temporaryHold === false || value.eventBasedHold === false) && !value.releaseIntent)
      return yield* storageError(
        "setHolds",
        "gcs",
        { _tag: "InvalidInput", field: "releaseIntent" },
        "not-started",
      );
    const address = yield* target(storage, { ref: value.ref }, "setHolds");
    if (value.ref.version._tag !== "GcsGeneration" || !value.ref.version.metageneration)
      return yield* storageError(
        "setHolds",
        "gcs",
        { _tag: "InvalidInput", field: "metageneration" },
        "not-started",
      );
    return (yield* jsonRequest(storage, "setHolds", true, {
      uri: address.uri,
      method: "PATCH",
      qs: { ...address.qs, ifMetagenerationMatch: value.ref.version.metageneration },
      json: {
        ...(value.temporaryHold !== undefined ? { temporaryHold: value.temporaryHold } : {}),
        ...(value.eventBasedHold !== undefined ? { eventBasedHold: value.eventBasedHold } : {}),
      },
    })).body;
  }),
});
export class GcsProtection extends Context.Service<GcsProtection, ReturnType<typeof make>>()(
  "@effect-kit/storage/gcs/GcsProtection",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(GcsProtection, Effect.map(GcsStorage, make));
}

export { GcsStorageTier } from "./tier.js";
