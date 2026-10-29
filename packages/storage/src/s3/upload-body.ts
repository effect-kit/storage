import { PassThrough } from "node:stream";

import { Effect, Predicate, Stream } from "effect";

import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import type { PutRequest } from "../requests.js";
import type { S3Connection } from "./config.js";

const collectUnknown = Effect.fnUntraced(function* <E, R>(
  source: Stream.Stream<Uint8Array, E, R>,
  limit: number,
  operation: string,
) {
  const chunks: Uint8Array[] = [];
  let total = 0;
  yield* Stream.runForEach(source, (chunk) =>
    Effect.gen(function* () {
      if (!Predicate.isUint8Array(chunk))
        return yield* storageError(
          operation,
          "s3",
          { _tag: "InvalidInput", field: "body" },
          "not-started",
        );
      if (chunk.byteLength > limit - total)
        return yield* storageError(
          operation,
          "s3",
          { _tag: "LimitExceeded", limit },
          "not-started",
        );
      chunks.push(Uint8Array.from(chunk));
      total += chunk.length;
    }),
  );

  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
});

export const uploadBody = Effect.fnUntraced(function* <A, E, R>(
  connection: S3Connection,
  input: Pick<PutRequest<E, R>, "body" | "contentLength" | "maxBytes">,
  operation: string,
  execute: (body: Uint8Array | PassThrough, length: number) => Effect.Effect<A, StorageError>,
): Effect.fn.Return<A, StorageError | E, R> {
  if (!Predicate.isUint8Array(input.body) && !Stream.isStream(input.body))
    return yield* storageError(
      operation,
      "s3",
      { _tag: "InvalidInput", field: "body" },
      "not-started",
    );
  const limit = input.maxBytes ?? Number.MAX_SAFE_INTEGER;
  if (input.contentLength !== undefined && input.contentLength > limit) {
    return yield* storageError(operation, "s3", { _tag: "LimitExceeded", limit }, "not-started");
  }

  if (Predicate.isUint8Array(input.body) || input.contentLength === undefined) {
    const bytes = Predicate.isUint8Array(input.body)
      ? input.body
      : yield* collectUnknown(
          input.body,
          Math.min(limit, connection.config.maxUnknownLengthBytes),
          operation,
        );
    if (bytes.length > limit)
      return yield* storageError(operation, "s3", { _tag: "LimitExceeded", limit }, "not-started");
    if (input.contentLength !== undefined && bytes.length !== input.contentLength) {
      return yield* storageError(
        operation,
        "s3",
        { _tag: "LengthMismatch", expected: input.contentLength, actual: bytes.length },
        "not-started",
      );
    }
    return yield* execute(bytes, bytes.length);
  }

  const source = input.body;
  const expected = input.contentLength;
  return yield* Effect.acquireUseRelease(
    Effect.sync(() => new PassThrough({ highWaterMark: 64 * 1024 })),
    (body) => {
      let total = 0;
      const producer = Stream.runForEach(source, (chunk) =>
        Effect.gen(function* () {
          if (!Predicate.isUint8Array(chunk))
            return yield* storageError(
              operation,
              "s3",
              { _tag: "InvalidInput", field: "body" },
              "unknown",
            );
          if (chunk.length > expected - total)
            return yield* storageError(
              operation,
              "s3",
              { _tag: "LengthMismatch", expected, actual: total + chunk.length },
              "unknown",
            );
          total += chunk.length;
          yield* Effect.tryPromise({
            try: (signal) =>
              new Promise<void>((resolve, reject) => {
                const cleanup = () => {
                  body.off("error", failed);
                  body.off("drain", drained);
                  signal.removeEventListener("abort", aborted);
                };
                const failed = () => {
                  cleanup();
                  reject(storageError(operation, "s3", { _tag: "TransportFailure" }, "unknown"));
                };
                const drained = () => {
                  cleanup();
                  resolve();
                };
                const aborted = () => {
                  cleanup();
                  body.destroy();
                  reject(storageError(operation, "s3", { _tag: "TransportFailure" }, "unknown"));
                };
                body.once("error", failed);
                signal.addEventListener("abort", aborted, { once: true });
                if (body.write(chunk)) drained();
                else body.once("drain", drained);
              }),
            catch: () => storageError(operation, "s3", { _tag: "TransportFailure" }, "unknown"),
          });
        }),
      ).pipe(
        Effect.andThen(
          Effect.suspend(() => {
            if (total !== expected)
              return Effect.fail(
                storageError(
                  operation,
                  "s3",
                  { _tag: "LengthMismatch", expected, actual: total },
                  "unknown",
                ),
              );
            body.end();
            return Effect.never;
          }),
        ),
      );

      // Racing within Effect preserves the producer's original E/Cause and joins it on cancellation.
      return Effect.raceFirst(execute(body, expected), producer);
    },
    (body) =>
      Effect.sync(() => {
        body.destroy();
      }),
  );
});
