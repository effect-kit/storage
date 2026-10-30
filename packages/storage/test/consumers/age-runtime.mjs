import assert from "node:assert/strict";

import { Effect, Redacted, Stream } from "effect";

import { AgeEncryption } from "@effect-kit/storage/encryption/age";
import { generateX25519Identity, identityToRecipient } from "age-encryption";

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const identity = yield* Effect.promise(() => generateX25519Identity());
      const recipient = yield* Effect.promise(() => identityToRecipient(identity));
      const codec = yield* AgeEncryption.make({
        recipients: [recipient],
        identities: Effect.succeed([Redacted.make(identity)]),
      });
      const input = new TextEncoder().encode("packed age codec");
      const encoded = yield* Stream.runCollect(codec.encode(Stream.succeed(input)));
      const decoded = yield* Stream.runCollect(codec.decode(Stream.fromIterable(encoded)));
      assert.equal(new TextDecoder().decode(decoded[0]), "packed age codec");
    }),
  ),
);
