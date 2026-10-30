import { Effect, Layer, Redacted } from "effect";

import { VercelStorage } from "@effect-kit/storage/vercel";
import { VercelClient } from "@effect-kit/storage/vercel/client";
import { VercelSigning, VercelUploadGrants } from "@effect-kit/storage/vercel/signing";
import { VercelUploadHandler } from "@effect-kit/storage/vercel/uploads";

export const provider = VercelStorage.layer({
  storeId: "example",
  access: "private",
  credential: { mode: "token", token: Redacted.make("vercel_blob_rw_example_application") },
});
export const signing = VercelSigning.layer().pipe(Layer.provide(provider));
export const grant = Effect.gen(function* () {
  return yield* (yield* VercelUploadGrants).create({
    key: "avatar.png",
    expiresInSeconds: 60,
    maximumSizeInBytes: 1024,
  });
}).pipe(Effect.provide(signing));
export const client = VercelClient.upload({
  key: "avatar.png",
  file: new Blob(["a"]),
  access: "private",
  handleUploadUrl: "/api/upload",
});
export const handler = VercelUploadHandler.make({
  namespace: "callbacks",
  ownership: "exclusive",
  callbackUrl: "https://example.com/api/upload",
  authorize: () => Effect.succeed({ maximumSizeInBytes: 1024 }),
  onObserved: () => Effect.void,
}).pipe(Effect.provide(provider));
