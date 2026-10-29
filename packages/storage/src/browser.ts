import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto";
import { Layer } from "effect";

import { sha256 } from "./runtime/portable-hash.js";
import { TransferHash } from "./transfers/dependencies.js";

export { sha256 };
export { aes256Gcm } from "./runtime/web-protection.js";
export const layer = Layer.mergeAll(
  BrowserCrypto.layerSecureRandom,
  BrowserCrypto.layerDigest,
  BrowserCrypto.layerAead,
  Layer.succeed(TransferHash, sha256),
);
