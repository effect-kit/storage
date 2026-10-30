import { Effect, FileSystem, Path, Sink, Stream } from "effect";

import { storageError } from "../errors.js";
import { sha256 } from "../runtime/portable-hash.js";

/** Scoped files bound staging without collecting object bytes in memory. */
export const makeStaging = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  return Effect.fnUntraced(function* <E, R>(
    body: Stream.Stream<Uint8Array, E, R>,
    maximum: number,
  ) {
    const directory = yield* fs.makeTempDirectoryScoped().pipe(
      Effect.mapError(() =>
        storageError("stage", "representation", {
          _tag: "ProviderFailure",
          code: "TemporaryDirectory",
        }),
      ),
    );
    const filename = path.join(directory, "object");
    const hash = sha256.create();
    let size = 0;
    yield* body.pipe(
      Stream.tap((chunk) =>
        Effect.suspend(() => {
          size += chunk.length;
          if (size > maximum)
            return Effect.fail(
              storageError("stage", "representation", { _tag: "LimitExceeded", limit: maximum }),
            );
          hash.update(chunk);
          return Effect.void;
        }),
      ),
      Stream.run(
        fs.sink(filename, { mode: 0o600 }).pipe(
          Sink.mapError(() =>
            storageError("stage", "representation", {
              _tag: "ProviderFailure",
              code: "TemporaryFile",
            }),
          ),
        ),
      ),
    );
    return {
      path: filename,
      size,
      checksum: hash.finish(),
      stream: (offset = 0) =>
        fs.stream(filename, { offset, chunkSize: 65536 }).pipe(
          Stream.mapError(() =>
            storageError("stage", "representation", {
              _tag: "ProviderFailure",
              code: "TemporaryRead",
            }),
          ),
        ),
    };
  });
});
