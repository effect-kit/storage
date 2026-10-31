import { Context, Effect, Layer } from "effect";

import type { MinioConfig } from "../s3-compatible/config.js";
import { makeCompatible } from "../s3-compatible/service.js";
import { makeAdministration } from "../s3/admin.js";
import type { S3AdministrationService } from "../s3/admin.js";

export type MinioAdminConfig = Omit<MinioConfig, "bucket" | "storeId">;
export class MinioAdministration extends Context.Service<
  MinioAdministration,
  S3AdministrationService
>()("@effect-kit/storage/minio/MinioAdministration") {
  static make(config: MinioAdminConfig) {
    return makeCompatible("minio", { ...config, bucket: "administration" }).pipe(
      Effect.map(makeAdministration),
    );
  }
  static layer(config: MinioAdminConfig) {
    return Layer.effect(MinioAdministration, MinioAdministration.make(config));
  }
}
