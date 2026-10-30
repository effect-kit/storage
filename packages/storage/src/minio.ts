import { Context } from "effect";

import type { MinioConfig } from "./s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "./s3-compatible/service.js";
import type { S3StorageService } from "./s3.js";

export type { MinioConfig } from "./s3-compatible/config.js";

export class MinioStorage extends Context.Service<MinioStorage, S3StorageService>()(
  "@effect-kit/storage/minio/MinioStorage",
) {
  static make(config: MinioConfig) {
    return makeCompatible("minio", config);
  }
  static layer(config: MinioConfig) {
    return compatibleLayer(MinioStorage, MinioStorage.make(config));
  }
}
