import { Context } from "effect";

import type { RegionalConfig } from "../s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "../s3-compatible/service.js";
import type { S3StorageService } from "../s3/index.js";

export type { RegionalConfig as ScalewayConfig } from "../s3-compatible/config.js";

export class ScalewayStorage extends Context.Service<ScalewayStorage, S3StorageService>()(
  "@effect-kit/storage/scaleway/ScalewayStorage",
) {
  static make(config: RegionalConfig) {
    return makeCompatible("scaleway", config);
  }
  static layer(config: RegionalConfig) {
    return compatibleLayer(ScalewayStorage, ScalewayStorage.make(config));
  }
}
