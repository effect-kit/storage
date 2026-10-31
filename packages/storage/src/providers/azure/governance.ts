import { Context, Effect, Layer, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import { ObjectRef } from "../../core/object.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { VersionRef } from "../../lifecycle/index.js";
import { AzureStorage } from "./index.js";
import type { AzureStorageService } from "./index.js";
import { target } from "./mapping.js";
import { request } from "./transport.js";

const Hold = Schema.Struct({
  ref: VersionRef,
  enabled: Schema.Boolean,
  releaseIntent: Schema.optional(Schema.Literal("release-object-hold")),
});
const Policy = Schema.Struct({
  ref: VersionRef,
  expiresOn: Schema.Date,
  mode: Schema.Literals(["Locked", "Unlocked"]),
  intent: Schema.Literal("change-object-immutability"),
});
const make = (storage: AzureStorageService) => ({
  inspect: Effect.fnUntraced(function* (input: { readonly ref: ObjectRef }) {
    const value = yield* decodeRequest(
      Schema.Struct({ ref: ObjectRef }),
      input,
      "inspectProtection",
      "azure",
    );
    const address = yield* target(storage, { ref: value.ref }, "inspectProtection");
    const output = yield* request(storage, "inspectProtection", false, (signal) =>
      address.client.getProperties({ abortSignal: signal }),
    );
    return {
      legalHold: output.legalHold,
      immutabilityMode: output.immutabilityPolicyMode,
      immutabilityExpiresOn: output.immutabilityPolicyExpiresOn,
      encryptionScope: output.encryptionScope,
      serverEncrypted: output.isServerEncrypted,
    };
  }),
  setLegalHold: Effect.fnUntraced(function* (input: typeof Hold.Type) {
    const value = yield* decodeRequest(Hold, input, "setLegalHold", "azure", true);
    if (!value.enabled && !value.releaseIntent)
      return yield* storageError(
        "setLegalHold",
        "azure",
        { _tag: "InvalidInput", field: "releaseIntent" },
        "not-started",
      );
    const address = yield* target(storage, { ref: value.ref }, "setLegalHold");
    yield* request(storage, "setLegalHold", true, (signal) =>
      address.client.setLegalHold(value.enabled, { abortSignal: signal }),
    );
  }),
  setImmutability: Effect.fnUntraced(function* (input: typeof Policy.Type) {
    const value = yield* decodeRequest(Policy, input, "setImmutability", "azure", true);
    const address = yield* target(storage, { ref: value.ref }, "setImmutability");
    yield* request(storage, "setImmutability", true, (signal) =>
      address.client.setImmutabilityPolicy(
        { expiriesOn: value.expiresOn, policyMode: value.mode },
        { abortSignal: signal },
      ),
    );
  }),
});
export class AzureProtection extends Context.Service<AzureProtection, ReturnType<typeof make>>()(
  "@effect-kit/storage/azure/AzureProtection",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(AzureProtection, Effect.map(AzureStorage, make));
}
