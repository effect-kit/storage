import { Effect, Redacted, Schema } from "effect";
import { Aead, SecureRandom } from "effect/crypto";
import { Base64 } from "effect/encoding";

import type { CheckpointProtection } from "../sessions/store.js";
import { sessionError } from "../sessions/store.js";

const Envelope = Schema.fromJsonString(
  Schema.Struct({
    v: Schema.Literal(1),
    iv: Schema.String,
    tag: Schema.String,
    body: Schema.String.check(Schema.isMaxLength(64 * 1024 * 1024)),
  }),
);
const utf8 = new TextEncoder();

export const aes256Gcm = Effect.fnUntraced(function* (secret: Redacted.Redacted<Uint8Array>) {
  const cipher = yield* Aead.Aead;
  const random = yield* SecureRandom.SecureRandom;
  const key = yield* Aead.key(Aead.aes256Gcm, Redacted.value(secret)).pipe(
    Effect.mapError(() => sessionError("CheckpointProtection")),
  );
  const protection: CheckpointProtection = {
    seal: (plaintext, aad) =>
      Effect.gen(function* () {
        const nonce = yield* random.randomBytes(12);
        const bytes = yield* cipher.encrypt(key, utf8.encode(plaintext), {
          nonce,
          aad: utf8.encode(aad),
        });
        return Redacted.make(
          JSON.stringify({
            v: 1,
            iv: Base64.encode(nonce),
            tag: Base64.encode(bytes.subarray(-16)),
            body: Base64.encode(bytes.subarray(0, -16)),
          }),
        );
      }).pipe(Effect.mapError(() => sessionError("CheckpointProtection"))),
    open: (encoded, aad) =>
      Effect.gen(function* () {
        const envelope = yield* Schema.decodeUnknownEffect(Envelope)(Redacted.value(encoded));
        const nonce = yield* Effect.fromResult(Base64.decode(envelope.iv));
        const body = yield* Effect.fromResult(Base64.decode(envelope.body));
        const tag = yield* Effect.fromResult(Base64.decode(envelope.tag));
        if (tag.length !== 16) return yield* sessionError("CheckpointProtection");
        const ciphertext = new Uint8Array(body.length + tag.length);
        ciphertext.set(body);
        ciphertext.set(tag, body.length);
        const plaintext = yield* cipher.decrypt(key, ciphertext, { nonce, aad: utf8.encode(aad) });
        return yield* Effect.try({
          try: () => new TextDecoder("utf-8", { fatal: true }).decode(plaintext),
          catch: () => sessionError("CheckpointProtection"),
        });
      }).pipe(Effect.mapError(() => sessionError("CheckpointProtection"))),
  };
  return protection;
});
