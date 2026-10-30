import { Context } from "effect";

import type { SpacesConfig } from "./s3-compatible/config.js";
import { compatibleLayer, makeCompatible } from "./s3-compatible/service.js";
import type { S3StorageService } from "./s3.js";

export type { SpacesConfig } from "./s3-compatible/config.js";

export class SpacesStorage extends Context.Service<SpacesStorage, S3StorageService>()(
  "@effect-kit/storage/spaces/SpacesStorage",
) {
  static make(config: SpacesConfig) {
    return makeCompatible("spaces", config);
  }
  static layer(config: SpacesConfig) {
    return compatibleLayer(SpacesStorage, SpacesStorage.make(config));
  }
}
