import { Context, Effect, Layer, Schema } from "effect";

import { GcsStorage } from "../gcs.js";
import type { GcsStorageService } from "../gcs.js";
import { decodeRequest } from "../internal/request-validation.js";
import { VersionRef } from "../lifecycle.js";

const Transition = Schema.Struct({
  ref: VersionRef,
  storageClass: Schema.Literals(["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"]),
  token: Schema.optional(Schema.NonEmptyString),
  intent: Schema.Literal("rewrite-storage-tier"),
});
const make = (storage: GcsStorageService) => ({
  // GCS Archive remains online: a rewrite is a tier transition, not an offline restore.
  transition: Effect.fnUntraced(function* (input: typeof Transition.Type) {
    const value = yield* decodeRequest(Transition, input, "transitionTier", "gcs", true);
    return yield* storage.rewrite({
      source: value.ref,
      destinationKey: value.ref.key,
      storageClass: value.storageClass,
      preconditions: { ifGenerationMatch: value.ref.version.id },
      ...(value.token ? { token: value.token } : {}),
    });
  }),
});
export class GcsStorageTier extends Context.Service<GcsStorageTier, ReturnType<typeof make>>()(
  "@effect-kit/storage/gcs/GcsStorageTier",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(GcsStorageTier, Effect.map(GcsStorage, make));
}
