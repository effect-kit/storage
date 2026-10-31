import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto";
import { Layer } from "effect";

import { TransferHash } from "../transfers/dependencies.js";
import { sha256 } from "./portable-hash.js";

export { sha256 };
export { aes256Gcm } from "./web-protection.js";
export const layer = Layer.mergeAll(
  BrowserCrypto.layerSecureRandom,
  BrowserCrypto.layerDigest,
  BrowserCrypto.layerAead,
  Layer.succeed(TransferHash, sha256),
);
