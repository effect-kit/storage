import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

import { Effect, Redacted, Schema } from "effect";

import type { CheckpointProtection } from "../store.js";
import { sessionError } from "../store.js";

const Ciphertext = Schema.Struct({
  v: Schema.Literal(1),
  iv: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9+/]{16}$/u)),
  tag: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9+/]{22}==$/u)),
  body: Schema.String.check(Schema.isMaxLength(64 * 1024 * 1024)),
});

export function aes256Gcm(key: Redacted.Redacted<Uint8Array>): CheckpointProtection {
  const bytes = Uint8Array.from(Redacted.value(key));
  return {
    seal: (text, aad) =>
      Effect.try({
        try: () => {
          if (bytes.length !== 32) throw new Error("Invalid key");
          const iv = randomBytes(12);
          const cipher = createCipheriv("aes-256-gcm", bytes, iv);
          cipher.setAAD(Buffer.from(aad));
          const encrypted = Buffer.concat([cipher.update(text, "utf8"), cipher.final()]);
          return Redacted.make(
            JSON.stringify({
              v: 1,
              iv: iv.toString("base64"),
              tag: cipher.getAuthTag().toString("base64"),
              body: encrypted.toString("base64"),
            }),
          );
        },
        catch: () => sessionError("CheckpointProtection"),
      }),
    open: (encoded, aad) =>
      Effect.try({
        try: () => {
          const value = Schema.decodeUnknownSync(Ciphertext)(JSON.parse(Redacted.value(encoded)));
          if (value.v !== 1 || bytes.length !== 32) throw new Error("Invalid protection version");
          const cipher = createDecipheriv("aes-256-gcm", bytes, Buffer.from(value.iv, "base64"));
          cipher.setAAD(Buffer.from(aad));
          cipher.setAuthTag(Buffer.from(value.tag, "base64"));
          return Buffer.concat([
            cipher.update(Buffer.from(value.body, "base64")),
            cipher.final(),
          ]).toString("utf8");
        },
        catch: () => sessionError("CheckpointProtection"),
      }),
  };
}
