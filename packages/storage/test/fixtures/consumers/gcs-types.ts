import { Effect, Layer } from "effect";

import { StorageSigning } from "@effect-kit/storage";
import { GcsStorage } from "@effect-kit/storage/gcs";
import { GcsPost } from "@effect-kit/storage/gcs/post";
import { GcsSigning } from "@effect-kit/storage/gcs/signing";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";

const provider = GcsStorage.layer({ bucket: "test-bucket" });
export const signing = Layer.merge(GcsSigning.layer(), GcsPost.layer()).pipe(
  Layer.provide(provider),
);
export const upload = Effect.gen(function* () {
  const transfers = yield* Transfers;
  return yield* transfers.upload({
    key: "example",
    source: UploadSource.bytes(new Uint8Array([1])),
    strategy: "offset",
  });
});
export const sign = Effect.gen(function* () {
  const service = yield* StorageSigning;
  return yield* service.sign({
    ref: { storeId: "gcs", key: "example" },
    method: "GET",
    expiresInSeconds: 60,
  });
}).pipe(Effect.provide(signing));

import { GcsVersions, GcsMetadata, GcsRecovery } from "@effect-kit/storage/gcs/lifecycle";
export const lifecycleCapabilities = [GcsVersions, GcsMetadata, GcsRecovery] as const;

import { GcsProtection, GcsStorageTier } from "@effect-kit/storage/gcs/governance";
export const governanceCapabilities = [GcsProtection, GcsStorageTier] as const;

import { GcsAdministration } from "@effect-kit/storage/gcs/admin";
export const adminCapabilities = [GcsAdministration] as const;

import { GcsEvents } from "@effect-kit/storage/gcs/events";
export const eventsCapabilities = [GcsEvents] as const;
