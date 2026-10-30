import { Effect, Layer, Redacted, Stream } from "effect";
import type { Scope } from "effect";

import { Decrypter, Encrypter } from "age-encryption";

import { StorageEncryptionCodec } from "../encryption.js";
import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import type { RepresentationCodec } from "../representation.js";

export interface AgeEncryptionOptions<R = never> {
  readonly recipients: ReadonlyArray<string>;
  /** Resolve current and retained rotation identities at operation time. */
  readonly identities: Effect.Effect<ReadonlyArray<Redacted.Redacted<string>>, StorageError, R>;
}
const fail = () => storageError("age", "encryption", { _tag: "IntegrityMismatch" });

export const AgeEncryption = {
  make: Effect.fnUntraced(function* <R>(
    options: AgeEncryptionOptions<R>,
  ): Effect.fn.Return<RepresentationCodec, StorageError, R> {
    if (!options.recipients.length)
      return yield* storageError("age", "encryption", {
        _tag: "InvalidConfiguration",
        field: "recipients",
      });
    const context = yield* Effect.context<R>();
    const transform =
      (encode: boolean) => (body: Stream.Stream<Uint8Array, StorageError, Scope.Scope>) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const readable = yield* Stream.toReadableStreamEffect(body);
            const identities = encode ? [] : yield* Effect.provide(options.identities, context);
            const output = yield* Effect.tryPromise({
              try: async () => {
                if (encode) {
                  const encrypter = new Encrypter();
                  for (const recipient of options.recipients) encrypter.addRecipient(recipient);
                  return encrypter.encrypt(readable);
                }
                const decrypter = new Decrypter();
                for (const identity of identities) decrypter.addIdentity(Redacted.value(identity));
                return decrypter.decrypt(readable);
              },
              catch: fail,
            });
            return Stream.fromReadableStream({ evaluate: () => output, onError: fail });
          }),
        );
    return { format: "effect-storage-age-v1", encode: transform(true), decode: transform(false) };
  }),
  layer: <R>(options: AgeEncryptionOptions<R>) =>
    Layer.effect(StorageEncryptionCodec, AgeEncryption.make(options)),
};
