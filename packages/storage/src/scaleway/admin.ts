import { Context, Effect, Layer } from "effect";

import type { RegionalConfig } from "../s3-compatible/config.js";
import { makeCompatible } from "../s3-compatible/service.js";
import { makeAdministration } from "../s3/admin.js";
import type { S3AdministrationService } from "../s3/admin.js";

export type ScalewayAdminConfig = Omit<RegionalConfig, "bucket" | "storeId">;
export class ScalewayAdministration extends Context.Service<
  ScalewayAdministration,
  S3AdministrationService
>()("@effect-kit/storage/scaleway/ScalewayAdministration") {
  static make(config: ScalewayAdminConfig) {
    return makeCompatible("scaleway", { ...config, bucket: "administration" }).pipe(
      Effect.map(makeAdministration),
    );
  }
  static layer(config: ScalewayAdminConfig) {
    return Layer.effect(ScalewayAdministration, ScalewayAdministration.make(config));
  }
}
