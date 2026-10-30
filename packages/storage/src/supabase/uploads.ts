import { Clock, DateTime, Effect, Layer, Redacted } from "effect";

import { UploadGrantDriver } from "../uploads/driver.js";
import { failUpload } from "../uploads/schema.js";
import { SupabaseUploadGrants } from "./signing.js";
export const SupabaseAuthorizedUploads = Layer.effect(
  UploadGrantDriver,
  Effect.gen(function* () {
    const grants = yield* SupabaseUploadGrants;
    return UploadGrantDriver.of({
      maximumGrantLifetimeMillis: 7260000,
      begin: (record) =>
        ["auto", "single"].includes(record.input.strategy ?? "auto")
          ? Effect.succeed(null)
          : Effect.fail(failUpload("Unsupported")),
      grant: Effect.fnUntraced(function* (record, expiresAt) {
        const grant = yield* grants.create({ key: record.stagingKey, upsert: false });
        // Native Supabase upload tokens have a fixed lifetime. The workflow must retain that full cleanup horizon.
        if (
          DateTime.toEpochMillis(grant.expiresAt) > expiresAt ||
          DateTime.toEpochMillis(grant.expiresAt) <= (yield* Clock.currentTimeMillis)
        )
          return yield* failUpload("Unsupported");
        return {
          _tag: "SignedRequest",
          constraints: grant.constraints,
          method: "PUT",
          url: Redacted.value(grant.url),
          headers: Redacted.value(grant.headers),
          expiresAt: DateTime.toEpochMillis(grant.expiresAt),
        };
      }),
      abort: () => Effect.void,
    });
  }),
);
