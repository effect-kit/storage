import { Context } from "effect";

import type { R2Config } from "./s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "./s3-compatible/service.js";
import type { S3StorageService } from "./s3.js";

export type { R2Config } from "./s3-compatible/config.js";

export class R2Storage extends Context.Service<R2Storage, S3StorageService>()(
  "@effect-kit/storage/r2/R2Storage",
) {
  static make(config: R2Config) {
    return makeCompatible("r2", config);
  }
  static layer(config: R2Config) {
    return compatibleLayer(R2Storage, R2Storage.make(config));
  }
}
