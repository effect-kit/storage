import * as DenoCrypto from "@effect/platform-deno/DenoCrypto";
import * as DenoFileSystem from "@effect/platform-deno/DenoFileSystem";
import * as DenoPath from "@effect/platform-deno/DenoPath";
import { Layer } from "effect";

import { TransferHash } from "../transfers/dependencies.js";
import { sha256 } from "./portable-hash.js";
import { fileSessionStore } from "./session-store.js";
import type { FileSessionStoreOptions } from "./session-store.js";
import { aes256Gcm } from "./web-protection.js";

export { sha256, aes256Gcm };
export { atomicFileStore } from "./filesystem.js";
export { fileDownloadTarget } from "./file-target.js";
export type { FileSessionStoreOptions } from "./session-store.js";

export const layer = Layer.mergeAll(
  DenoFileSystem.layer,
  DenoPath.layer,
  DenoCrypto.layerSecureRandom,
  DenoCrypto.layerDigest,
  DenoCrypto.layerAead,
  Layer.succeed(TransferHash, sha256),
);
export const sessionStore = (options: FileSessionStoreOptions) =>
  fileSessionStore(options, aes256Gcm(options.key));
