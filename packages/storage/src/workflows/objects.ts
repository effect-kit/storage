import { Effect, Stream } from "effect";

import { storageError } from "../errors.js";
import type { ObjectMetadata } from "../object.js";
import { sha256 } from "../runtime/portable-hash.js";
import type { StorageService } from "../storage.js";

export const pin = (metadata: ObjectMetadata) =>
  metadata.ref.version
    ? Effect.succeed({ ref: metadata.ref })
    : metadata.etag
      ? Effect.succeed({
          ref: metadata.ref,
          condition: { _tag: "IfMatch" as const, etag: metadata.etag },
        })
      : Effect.fail(
          storageError("verify", "workflow", { _tag: "Unsupported", capability: "pinned-read" }),
        );
export const hashObject = Effect.fnUntraced(function* (
  storage: StorageService,
  metadata: ObjectMetadata,
) {
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const object = yield* storage.get(yield* pin(metadata));
      if (object._tag !== "Object")
        return yield* storageError("verify", storage.provider, { _tag: "IntegrityMismatch" });
      const hash = sha256.create();
      let size = 0;
      yield* Stream.runForEach(object.body, (chunk) =>
        Effect.sync(() => {
          size += chunk.length;
          hash.update(chunk);
        }),
      );
      if (metadata.size !== undefined && size !== metadata.size)
        return yield* storageError("verify", storage.provider, {
          _tag: "LengthMismatch",
          expected: metadata.size,
          actual: size,
        });
      return { size, sha256: hash.finish().value };
    }),
  );
});
export const deletePinned = Effect.fnUntraced(function* (
  storage: StorageService,
  metadata: ObjectMetadata,
) {
  if (metadata.ref.version)
    return yield* storage
      .delete({ ref: metadata.ref })
      .pipe(Effect.catchReason("StorageError", "NotFound", () => Effect.void));
  if (!metadata.etag || !storage.capabilities.deleteConditions.includes("if-match"))
    return yield* storageError(
      "delete",
      storage.provider,
      {
        _tag: "Unsupported",
        capability: "pinned-delete",
      },
      "not-started",
    );
  yield* storage
    .delete({ ref: metadata.ref, condition: { _tag: "IfMatch", etag: metadata.etag } })
    .pipe(Effect.catchReason("StorageError", "NotFound", () => Effect.void));
});
export const headOptional = (storage: StorageService, key: string) =>
  storage
    .head({ key })
    .pipe(Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(undefined)));
