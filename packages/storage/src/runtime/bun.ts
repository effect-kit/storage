import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunPath from "@effect/platform-bun/BunPath";
import { Effect, Layer } from "effect";

import { TransferHash } from "../transfers/dependencies.js";
import * as BunFileSystem from "./bun-filesystem.js";
import { sha256 } from "./node-hash.js";
import { aes256Gcm } from "./node-protection.js";
import { fileSessionStore } from "./session-store.js";
import type { FileSessionStoreOptions } from "./session-store.js";

export { sha256, aes256Gcm };
export { atomicFileStore } from "./filesystem.js";
export { fileDownloadTarget } from "./file-target.js";
export type { FileSessionStoreOptions } from "./session-store.js";

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
