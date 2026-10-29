import * as DenoCrypto from "@effect/platform-deno/DenoCrypto";
import * as DenoFileSystem from "@effect/platform-deno/DenoFileSystem";
import * as DenoPath from "@effect/platform-deno/DenoPath";
import { Layer } from "effect";

import { sha256 } from "./runtime/portable-hash.js";
import { fileSessionStore } from "./runtime/session-store.js";
import type { FileSessionStoreOptions } from "./runtime/session-store.js";
import { aes256Gcm } from "./runtime/web-protection.js";
import { TransferHash } from "./transfers/dependencies.js";

export { sha256, aes256Gcm };
export { atomicFileStore } from "./runtime/filesystem.js";
export { fileDownloadTarget } from "./runtime/file-target.js";
export type { FileSessionStoreOptions } from "./runtime/session-store.js";

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
