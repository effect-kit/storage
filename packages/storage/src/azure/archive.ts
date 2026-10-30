import { Context, Effect, Layer, Schema } from "effect";

import { AzureStorage } from "../azure.js";
import type { AzureStorageService } from "../azure.js";
import { decodeRequest } from "../internal/request-validation.js";
import { ObjectRef } from "../object.js";
import { target } from "./mapping.js";
import { request } from "./transport.js";

const Tier = Schema.Struct({
  ref: ObjectRef,
  tier: Schema.Literals(["Hot", "Cool", "Cold", "Archive"]),
  priority: Schema.optional(Schema.Literals(["Standard", "High"])),
});
const make = (storage: AzureStorageService) => ({
  setTier: Effect.fnUntraced(function* (input: typeof Tier.Type) {
    const value = yield* decodeRequest(Tier, input, "setTier", "azure", true);
    const address = yield* target(storage, { ref: value.ref }, "setTier");
    yield* request(storage, "setTier", true, (signal) =>
      address.client.setAccessTier(value.tier, {
        abortSignal: signal,
        ...(value.priority ? { rehydratePriority: value.priority } : {}),
      }),
    );
    return { _tag: "Accepted" as const };
  }),
  restoreStatus: Effect.fnUntraced(function* (input: { readonly ref: ObjectRef }) {
    const value = yield* decodeRequest(
      Schema.Struct({ ref: ObjectRef }),
      input,
      "restoreStatus",
      "azure",
    );
    const address = yield* target(storage, { ref: value.ref }, "restoreStatus");
    const output = yield* request(storage, "restoreStatus", false, (signal) =>
      address.client.getProperties({ abortSignal: signal }),
    );
    return {
      _tag: output.archiveStatus
        ? ("Pending" as const)
        : output.accessTier === "Archive"
          ? ("Archived" as const)
          : ("Ready" as const),
      tier: output.accessTier,
      rehydration: output.archiveStatus,
    };
  }),
  undelete: Effect.fnUntraced(function* (input: { readonly ref: ObjectRef }) {
    const value = yield* decodeRequest(
      Schema.Struct({ ref: ObjectRef }),
      input,
      "undelete",
      "azure",
      true,
    );
    const address = yield* target(storage, { ref: value.ref }, "undelete");
    yield* request(storage, "undelete", true, (signal) =>
      address.client.undelete({ abortSignal: signal }),
    );
    return { _tag: "Accepted" as const };
  }),
});
export class AzureArchive extends Context.Service<AzureArchive, ReturnType<typeof make>>()(
  "@effect-kit/storage/azure/AzureArchive",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(AzureArchive, Effect.map(AzureStorage, make));
}
