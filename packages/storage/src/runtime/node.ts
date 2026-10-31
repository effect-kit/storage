import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem";
import * as NodePath from "@effect/platform-node/NodePath";
import { Effect, Layer } from "effect";

import { TransferHash } from "../transfers/dependencies.js";
import { sha256 } from "./node-hash.js";
import { aes256Gcm } from "./node-protection.js";
import { fileSessionStore } from "./session-store.js";
import type { FileSessionStoreOptions } from "./session-store.js";

export { sha256, aes256Gcm };
export { atomicFileStore } from "./filesystem.js";
export { fileDownloadTarget } from "./file-target.js";
export type { FileSessionStoreOptions } from "./session-store.js";

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
