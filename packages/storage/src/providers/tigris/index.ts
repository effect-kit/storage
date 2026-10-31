import { Context } from "effect";

import type { TigrisConfig } from "../s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "../s3-compatible/service.js";
import type { S3StorageService } from "../s3/index.js";

export type { TigrisConfig } from "../s3-compatible/config.js";

export class TigrisStorage extends Context.Service<TigrisStorage, S3StorageService>()(
  "@effect-kit/storage/tigris/TigrisStorage",
) {
  static make(config: TigrisConfig) {
    return makeCompatible("tigris", config);
  }
  static layer(config: TigrisConfig) {
    return compatibleLayer(TigrisStorage, TigrisStorage.make(config));
  }
}
