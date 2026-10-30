import { Context, Effect, Layer } from "effect";

import type { RegionalConfig } from "../s3-compatible/config.js";
import { makeCompatible } from "../s3-compatible/service.js";
import { makeAdministration } from "../s3/admin.js";
import type { S3AdministrationService } from "../s3/admin.js";

export type WasabiAdminConfig = Omit<RegionalConfig, "bucket" | "storeId">;
export class WasabiAdministration extends Context.Service<
  WasabiAdministration,
  S3AdministrationService
>()("@effect-kit/storage/wasabi/WasabiAdministration") {
  static make(config: WasabiAdminConfig) {
    return makeCompatible("wasabi", { ...config, bucket: "administration" }).pipe(
      Effect.map(makeAdministration),
    );
  }
  static layer(config: WasabiAdminConfig) {
    return Layer.effect(WasabiAdministration, WasabiAdministration.make(config));
  }
}
