import { Context } from "effect";

import type { RegionalConfig } from "./s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "./s3-compatible/service.js";
import type { S3StorageService } from "./s3.js";

export type { RegionalConfig as B2Config } from "./s3-compatible/config.js";

export class B2Storage extends Context.Service<B2Storage, S3StorageService>()(
  "@effect-kit/storage/b2/B2Storage",
) {
  static make(config: RegionalConfig) {
    return makeCompatible("b2", config);
  }
  static layer(config: RegionalConfig) {
    return compatibleLayer(B2Storage, B2Storage.make(config));
  }
}
