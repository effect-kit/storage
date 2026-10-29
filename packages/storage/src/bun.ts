import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunFileSystem from "@effect/platform-bun/BunFileSystem";
import * as BunPath from "@effect/platform-bun/BunPath";
import { Effect, Layer } from "effect";

import { sha256 } from "./runtime/node-hash.js";
import { aes256Gcm } from "./runtime/node-protection.js";
import { fileSessionStore } from "./runtime/session-store.js";
import type { FileSessionStoreOptions } from "./runtime/session-store.js";
import { TransferHash } from "./transfers/dependencies.js";

export { sha256, aes256Gcm };
export { atomicFileStore } from "./runtime/filesystem.js";
export { fileDownloadTarget } from "./runtime/file-target.js";
export type { FileSessionStoreOptions } from "./runtime/session-store.js";

export const layer = Layer.mergeAll(
  BunFileSystem.layer,
  BunPath.layer,
  BunCrypto.layerSecureRandom,
  BunCrypto.layerDigest,
  Layer.succeed(TransferHash, sha256),
);
export const sessionStore = (options: FileSessionStoreOptions) =>
  fileSessionStore(
    options,
    Effect.sync(() => aes256Gcm(options.key)),
  );
