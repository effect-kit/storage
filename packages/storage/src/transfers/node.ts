import { createHash } from "node:crypto";

import type { TransferDigest } from "./models.js";

/** Node's maintained streaming implementation; no whole-object WebCrypto buffering. */
export const sha256: TransferDigest = {
  algorithm: "sha256",
  workspaceBytes: 32768,
  create: () => {
    const hash = createHash("sha256");
    return {
      update: (bytes) => {
        hash.update(bytes);
      },
      snapshot: () => ({
        algorithm: "sha256",
        encoding: "hex",
        scope: "full-object",
        value: hash.copy().digest("hex"),
      }),
      finish: () => ({
        algorithm: "sha256",
        encoding: "hex",
        scope: "full-object",
        value: hash.digest("hex"),
      }),
    };
  },
};
