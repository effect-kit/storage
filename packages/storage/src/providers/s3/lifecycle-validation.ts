import { Effect } from "effect";

import { storageError } from "../../core/errors.js";
import type { ObjectRef } from "../../core/object.js";
import type { S3StorageService } from "./index.js";
import { validateRef } from "./object-mapping.js";

export const lifecycleTarget = Effect.fnUntraced(function* (
  storage: S3StorageService,
  ref: ObjectRef,
  operation: string,
  version = false,
) {
  yield* validateRef(ref, storage.config, operation);
  if (
    (version && (!ref.version || ref.version.id === "null")) ||
    (ref.version && storage.capabilities.versionedReads !== "history")
  )
    return yield* storageError(
      operation,
      storage.provider,
      { _tag: "Unsupported", capability: "native-history" },
      "not-started",
    );
  return {
    Bucket: storage.config.bucket,
    Key: ref.key,
    ...(ref.version ? { VersionId: ref.version.id } : {}),
  };
});
export const lifecycleFeature = (
  storage: S3StorageService,
  feature: "tags" | "versions" | "protection" | "archive",
) => {
  const provider = storage.provider;
  const supported =
    provider === "s3" ||
    (feature === "versions"
      ? storage.capabilities.versionedReads === "history"
      : feature === "tags"
        ? storage.profile?.tagging
        : feature === "protection"
          ? ["minio", "wasabi", "scaleway", "b2", "hetzner"].includes(provider)
          : provider === "tigris");
  return supported
    ? Effect.void
    : Effect.fail(
        storageError(
          feature,
          provider,
          { _tag: "Unsupported", capability: feature },
          "not-started",
        ),
      );
};
