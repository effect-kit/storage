import { Context, Effect, Layer } from "effect";

import type { HetznerConfig } from "../s3-compatible/config.js";
import { makeCompatible } from "../s3-compatible/service.js";
import { makeAdministration } from "../s3/admin.js";
import type { S3AdministrationService } from "../s3/admin.js";

export type HetznerAdminConfig = Omit<HetznerConfig, "bucket" | "storeId">;
export class HetznerAdministration extends Context.Service<
  HetznerAdministration,
  S3AdministrationService
>()("@effect-kit/storage/hetzner/HetznerAdministration") {
  static make(config: HetznerAdminConfig) {
    return makeCompatible("hetzner", { ...config, bucket: "administration" }).pipe(
      Effect.map(makeAdministration),
    );
  }
  static layer(config: HetznerAdminConfig) {
    return Layer.effect(HetznerAdministration, HetznerAdministration.make(config));
  }
}
