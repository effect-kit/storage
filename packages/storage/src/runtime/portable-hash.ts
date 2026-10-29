import { sha256 as hash } from "@noble/hashes/sha2.js";

import type { TransferDigest } from "../transfers/models.js";

const checksum = (bytes: Uint8Array) => ({
  algorithm: "sha256",
  encoding: "hex" as const,
  scope: "full-object" as const,
  value: Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(""),
});

export const sha256: TransferDigest = {
  algorithm: "sha256",
  workspaceBytes: 32768,
  create: () => {
    const state = hash.create();
    return {
      update: (bytes) => {
        state.update(bytes);
      },
      snapshot: () => checksum(state.clone().digest()),
      finish: () => checksum(state.digest()),
    };
  },
};
