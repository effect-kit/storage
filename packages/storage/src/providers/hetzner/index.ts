import { Context } from "effect";

import type { HetznerConfig } from "../s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "../s3-compatible/service.js";
import type { S3StorageService } from "../s3/index.js";

export type { HetznerConfig } from "../s3-compatible/config.js";

export class HetznerStorage extends Context.Service<HetznerStorage, S3StorageService>()(
  "@effect-kit/storage/hetzner/HetznerStorage",
) {
  static make(config: HetznerConfig) {
    return makeCompatible("hetzner", config);
  }
  static layer(config: HetznerConfig) {
    return compatibleLayer(HetznerStorage, HetznerStorage.make(config));
  }
}
