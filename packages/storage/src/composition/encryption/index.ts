import { Context, Effect, Layer } from "effect";

import type { RepresentationCodec, RepresentationOptions } from "../representation/index.js";
import { makeRepresentation, representationLayer } from "../representation/storage.js";

/** Supply a qualified authenticated format; the facade does not implement cryptography. */
export class StorageEncryptionCodec extends Context.Service<
  StorageEncryptionCodec,
  RepresentationCodec
>()("@effect-kit/storage/StorageEncryptionCodec") {}
export const StorageEncryption = {
  make: Effect.fnUntraced(function* (options: RepresentationOptions) {
    return yield* makeRepresentation(yield* StorageEncryptionCodec, options).pipe(
      Effect.map((value) => value.storage),
    );
  }),
  layer: (options: RepresentationOptions) =>
    Layer.unwrap(
      Effect.map(StorageEncryptionCodec, (codec) => representationLayer(codec, options)),
    ),
};
export { StorageRepresentation } from "../representation/index.js";
