import { Effect, Predicate, Stream } from "effect";

import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import type { PutRequest } from "../requests.js";

/** A bounded Web stream bridge that preserves source errors and waits for source EOF. */
export const webUploadBody = Effect.fnUntraced(function* <A, E, R>(
  provider: string,
  input: Pick<PutRequest<E, R>, "body" | "contentLength" | "maxBytes">,
  unknownLimit: number,
  execute: (
    body: Uint8Array | ReadableStream<Uint8Array>,
    length: number,
  ) => Effect.Effect<A, StorageError>,
): Effect.fn.Return<A, StorageError | E, R> {
  const failure = (expected: number, actual: number, started = false) =>
    storageError(
      "put",
      provider,
      { _tag: "LengthMismatch", expected, actual },
      started ? "unknown" : "not-started",
    );
  const limit = input.maxBytes ?? Number.MAX_SAFE_INTEGER;
  if (input.contentLength !== undefined && input.contentLength > limit)
    return yield* storageError("put", provider, { _tag: "LimitExceeded", limit }, "not-started");

  if (Predicate.isUint8Array(input.body) || input.contentLength === undefined) {
    let bytes: Uint8Array;
    if (Predicate.isUint8Array(input.body)) bytes = input.body;
    else {
      const chunks: Uint8Array[] = [];
      let total = 0;
      const bound = Math.min(limit, unknownLimit);
      yield* Stream.runForEach(input.body, (chunk) =>
        Effect.gen(function* () {
          if (!Predicate.isUint8Array(chunk))
            return yield* storageError(
              "put",
              provider,
              { _tag: "InvalidInput", field: "body" },
              "not-started",
            );
          if (chunk.length > bound - total)
            return yield* storageError(
              "put",
              provider,
              { _tag: "LimitExceeded", limit: bound },
              "not-started",
            );
          chunks.push(Uint8Array.from(chunk));
          total += chunk.length;
        }),
      );
      bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.length;
      }
    }
    if (bytes.length > limit)
      return yield* storageError("put", provider, { _tag: "LimitExceeded", limit }, "not-started");
    if (input.contentLength !== undefined && bytes.length !== input.contentLength)
      return yield* failure(input.contentLength, bytes.length);
    return yield* execute(bytes, bytes.length);
  }

  const stream = new TransformStream<Uint8Array, Uint8Array>();
  const writer = stream.writable.getWriter();
  const expected = input.contentLength;
  let total = 0;
  const write = (action: () => Promise<unknown>) =>
    Effect.tryPromise({
      try: action,
      catch: () => storageError("put", provider, { _tag: "TransportFailure" }, "unknown"),
    });
  const producer = Stream.runForEach(input.body, (chunk) =>
    Effect.gen(function* () {
      if (!Predicate.isUint8Array(chunk))
        return yield* storageError(
          "put",
          provider,
          { _tag: "InvalidInput", field: "body" },
          "unknown",
        );
      if (chunk.length > expected - total)
        return yield* failure(expected, total + chunk.length, true);
      total += chunk.length;
      yield* write(() => writer.write(chunk));
    }),
  ).pipe(
    Effect.andThen(
      Effect.suspend(() =>
        total === expected
          ? write(() => writer.close())
          : Effect.fail(failure(expected, total, true)),
      ),
    ),
  );

  return yield* Effect.acquireUseRelease(
    Effect.succeed(writer),
    () =>
      Effect.all([execute(stream.readable, expected), producer], { concurrency: 2 }).pipe(
        Effect.map(([result]) => result),
      ),
    () =>
      Effect.promise(() => writer.abort().catch(() => undefined)).pipe(
        Effect.ensuring(Effect.sync(() => writer.releaseLock())),
      ),
  );
});
