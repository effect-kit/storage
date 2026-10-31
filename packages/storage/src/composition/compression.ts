import { Effect, Layer, Stream } from "effect";

import { storageError } from "../core/errors.js";
import type { RepresentationCodec, RepresentationOptions } from "./representation/index.js";
import { makeRepresentation, representationLayer } from "./representation/storage.js";

const transform =
  (encode: boolean): RepresentationCodec["encode"] =>
  (body) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const readable = yield* Stream.toReadableStreamEffect(body);
        const transformed = yield* Effect.try({
          try: () =>
            readable.pipeThrough(
              (encode
                ? new CompressionStream("gzip")
                : new DecompressionStream("gzip")) as ReadableWritablePair<Uint8Array, Uint8Array>,
            ),
          catch: () =>
            storageError("compression", "web", { _tag: "Unsupported", capability: "gzip-stream" }),
        });
        return Stream.fromReadableStream({
          evaluate: () => transformed,
          onError: () => storageError("compression", "gzip", { _tag: "IntegrityMismatch" }),
        });
      }),
    );
const gzip: RepresentationCodec = {
  format: "effect-storage-gzip-v1",
  encode: transform(true),
  decode: transform(false),
};
export interface StorageCompressionOptions extends RepresentationOptions {
  readonly format: "gzip";
}
export const StorageCompression = {
  make: (options: StorageCompressionOptions) =>
    options.format === "gzip"
      ? makeRepresentation(gzip, options).pipe(Effect.map((value) => value.storage))
      : Effect.fail(
          storageError("compression", "web", { _tag: "InvalidConfiguration", field: "format" }),
        ),
  layer: (options: StorageCompressionOptions) =>
    options.format === "gzip"
      ? representationLayer(gzip, options)
      : Layer.unwrap(
          Effect.fail(
            storageError("compression", "web", { _tag: "InvalidConfiguration", field: "format" }),
          ),
        ),
};
export { StorageRepresentation } from "./representation/index.js";
