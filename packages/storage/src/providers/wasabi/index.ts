import { Context } from "effect";

import type { RegionalConfig } from "../s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "../s3-compatible/service.js";
import type { S3StorageService } from "../s3/index.js";

export type { RegionalConfig as WasabiConfig } from "../s3-compatible/config.js";

export class WasabiStorage extends Context.Service<WasabiStorage, S3StorageService>()(
  "@effect-kit/storage/wasabi/WasabiStorage",
) {
  static make(config: RegionalConfig) {
    return makeCompatible("wasabi", config);
  }
  static layer(config: RegionalConfig) {
    return compatibleLayer(WasabiStorage, WasabiStorage.make(config));
  }
}
