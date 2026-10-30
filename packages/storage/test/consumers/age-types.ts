import { Effect, Redacted } from "effect";

import type { StorageError } from "@effect-kit/storage";
import { StorageEncryption, StorageEncryptionCodec } from "@effect-kit/storage/encryption";
import { AgeEncryption } from "@effect-kit/storage/encryption/age";
import type { RepresentationCodec } from "@effect-kit/storage/representation";

const codec: Effect.Effect<RepresentationCodec, StorageError> = AgeEncryption.make({
  recipients: ["configured-recipient"],
  identities: Effect.succeed([Redacted.make("configured-identity")]),
});
void [codec, StorageEncryption.layer, StorageEncryptionCodec];
