import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
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
  NodeFileSystem.layer,
  NodePath.layer,
  NodeCrypto.layerSecureRandom,
  NodeCrypto.layerDigest,
  Layer.succeed(TransferHash, sha256),
);
export const sessionStore = (options: FileSessionStoreOptions) =>
  fileSessionStore(
    options,
    Effect.sync(() => aes256Gcm(options.key)),
  );
