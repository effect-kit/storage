import { Effect, Stream } from "effect";

import type { StorageError } from "../errors.js";

/** Emits at most afterBytes before failing, including when the boundary splits a chunk. */
export function failBody<E, R>(
  source: Stream.Stream<Uint8Array, E, R>,
  afterBytes: number,
  failure: StorageError,
): Stream.Stream<Uint8Array, E | StorageError, R> {
  return Stream.unwrap(
    Effect.sync(() => {
      let remaining = afterBytes;
      return Stream.flatMap(source, (chunk) => {
        if (remaining <= 0) return Stream.fail(failure);
        const allowed = Math.min(remaining, chunk.length);
        remaining -= allowed;
        return allowed < chunk.length || remaining === 0
          ? Stream.concat(Stream.succeed(chunk.slice(0, allowed)), Stream.fail(failure))
          : Stream.succeed(chunk);
      });
    }),
  );
}
