import { Context, Effect, Layer } from "effect";

import type { SpacesConfig } from "../s3-compatible/config.js";
import { makeCompatible } from "../s3-compatible/service.js";
import { makeAdministration } from "../s3/admin.js";
import type { S3AdministrationService } from "../s3/admin.js";

export type SpacesAdminConfig = Omit<SpacesConfig, "bucket" | "storeId">;
export class SpacesAdministration extends Context.Service<
  SpacesAdministration,
  S3AdministrationService
>()("@effect-kit/storage/spaces/SpacesAdministration") {
  static make(config: SpacesAdminConfig) {
    return makeCompatible("spaces", { ...config, bucket: "administration" }).pipe(
      Effect.map(makeAdministration),
    );
  }
  static layer(config: SpacesAdminConfig) {
    return Layer.effect(SpacesAdministration, SpacesAdministration.make(config));
  }
}
