import { Effect, Predicate, Stream } from "effect";

import { storageError } from "../../core/errors.js";
import type { StorageError } from "../../core/errors.js";
import type { UploadBody } from "../../core/requests.js";
import { call } from "./mapping.js";

const collect = Effect.fnUntraced(function* <E, R>(
  source: Stream.Stream<Uint8Array, E, R>,
  limit: number,
) {
  const chunks: Uint8Array[] = [];
  let length = 0;
  yield* Stream.runForEach(source, (chunk) =>
    Effect.gen(function* () {
      if (!Predicate.isUint8Array(chunk))
        return yield* storageError(
          "put",
          "r2-workers",
          { _tag: "InvalidInput", field: "body" },
          "not-started",
        );
      if (chunk.length > limit - length)
        return yield* storageError(
          "put",
          "r2-workers",
          { _tag: "LimitExceeded", limit },
          "not-started",
        );
      chunks.push(Uint8Array.from(chunk));
      length += chunk.length;
    }),
  );
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
});

interface LengthStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export const uploadBody = Effect.fnUntraced(function* <A, E, R>(
  input: {
    body: UploadBody<E, R>;
    contentLength?: number | undefined;
    maxBytes?: number | undefined;
  },
  maximumBytes: number,
  unknownLimit: number,
  execute: (body: Uint8Array | ReadableStream<Uint8Array>) => Effect.Effect<A, StorageError>,
): Effect.fn.Return<A, StorageError | E, R> {
  if (!Predicate.isUint8Array(input.body) && !Stream.isStream(input.body))
    return yield* storageError(
      "put",
      "r2-workers",
      { _tag: "InvalidInput", field: "body" },
      "not-started",
    );
  const limit = Math.min(input.maxBytes ?? maximumBytes, maximumBytes);
  if (input.contentLength !== undefined && input.contentLength > limit)
    return yield* storageError(
      "put",
      "r2-workers",
      { _tag: "LimitExceeded", limit },
      "not-started",
    );
  if (Predicate.isUint8Array(input.body) || input.contentLength === undefined) {
    const bytes = Predicate.isUint8Array(input.body)
      ? input.body
      : yield* collect(input.body, Math.min(limit, unknownLimit));
    if (bytes.length > limit)
      return yield* storageError(
        "put",
        "r2-workers",
        { _tag: "LimitExceeded", limit },
        "not-started",
      );
    if (input.contentLength !== undefined && input.contentLength !== bytes.length)
      return yield* storageError(
        "put",
        "r2-workers",
        { _tag: "LengthMismatch", expected: input.contentLength, actual: bytes.length },
        "not-started",
      );
    return yield* execute(bytes);
  }

  const Constructor = (
    globalThis as unknown as { FixedLengthStream?: new (length: number) => LengthStream }
  ).FixedLengthStream;
  if (!Constructor)
    return yield* storageError(
      "put",
      "r2-workers",
      { _tag: "Unsupported", capability: "WorkerFixedLengthStream" },
      "not-started",
    );
  const stream = new Constructor(input.contentLength);
  const writer = stream.writable.getWriter();
  const expected = input.contentLength;
  let length = 0;
  const producer = Stream.runForEach(input.body, (chunk) =>
    Effect.gen(function* () {
      if (!Predicate.isUint8Array(chunk))
        return yield* storageError(
          "put",
          "r2-workers",
          { _tag: "InvalidInput", field: "body" },
          "unknown",
        );
      if (chunk.length > expected - length)
        return yield* storageError(
          "put",
          "r2-workers",
          { _tag: "LengthMismatch", expected, actual: length + chunk.length },
          "unknown",
        );
      length += chunk.length;
      yield* call("put.stream", () => writer.write(chunk), "unknown");
    }),
  ).pipe(
    Effect.andThen(
      Effect.suspend(() =>
        length === expected
          ? call("put.stream", () => writer.close(), "unknown")
          : Effect.fail(
              storageError(
                "put",
                "r2-workers",
                { _tag: "LengthMismatch", expected, actual: length },
                "unknown",
              ),
            ),
      ),
    ),
  );

  return yield* Effect.acquireUseRelease(
    Effect.succeed(writer),
    () =>
      Effect.all([producer, execute(stream.readable)], { concurrency: 2 }).pipe(
        Effect.map(([, result]) => result),
      ),
    // Abort rejection means the local writer is already terminal. It cannot cancel remote R2 work.
    () =>
      Effect.promise(() =>
        writer.abort().then(
          () => undefined,
          () => undefined,
        ),
      ).pipe(Effect.ensuring(Effect.sync(() => writer.releaseLock()))),
  );
});
