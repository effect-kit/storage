import { Effect, Layer, Redacted } from "effect";

import { Storage } from "@effect-kit/storage";
import { SupabaseStorage } from "@effect-kit/storage/supabase";
import { SupabaseSigning, SupabaseUploadGrants } from "@effect-kit/storage/supabase/signing";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";

export const provider = SupabaseStorage.layer({
  url: "https://example.supabase.co",
  bucket: "files",
  storeId: "supabase:files:user",
  auth: { mode: "user", token: Redacted.make("application-user-jwt") },
});
export const signing = SupabaseSigning.layer().pipe(Layer.provide(provider));
export const grant = Effect.gen(function* () {
  return yield* (yield* SupabaseUploadGrants).create({ key: "avatar.png" });
}).pipe(Effect.provide(signing));
export const upload = Effect.gen(function* () {
  return yield* (yield* Transfers).upload({
    key: "a",
    source: UploadSource.bytes(new Uint8Array([1])),
  });
});
export const head = Effect.gen(function* () {
  return yield* (yield* Storage).head({ key: "a" });
}).pipe(Effect.provide(provider));
