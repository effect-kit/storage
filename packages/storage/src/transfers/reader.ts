import { Effect, Pull, Stream } from "effect";
import { Base64, Hex } from "effect/encoding";

import type { StorageError } from "../errors.js";
import { storageError } from "../errors.js";
import type { Checksum } from "../object.js";
import type { TransferDigest } from "./models.js";
import type { UploadSource } from "./sources.js";

const failure = (reason: StorageError["reason"]) =>
  storageError("source.read", "transfer", reason, "not-started");

export const makeReader = Effect.fnUntraced(function* <E, R>(
  source: UploadSource<E, R>,
  options: {
    readonly maximumBytes: number;
    readonly maximumChunkBytes: number;
    readonly digest: TransferDigest;
    readonly onRead: (count: number) => void;
  },
) {
  const pull = yield* Stream.toPull(source.open().pipe(Stream.rechunk(1)));
  const hash = options.digest.create();
  let pending: Uint8Array | undefined;
  let offset = 0;
  let total = 0;
  let ended = false;
  let checksum: Checksum | undefined;

  const next = Effect.fnUntraced(function* (
    capacity: number,
  ): Effect.fn.Return<Uint8Array | undefined, E | StorageError, R> {
    if (ended && !pending) return undefined;
    const buffer = new Uint8Array(Math.max(1, capacity));
    let used = 0;
    while (used < buffer.length) {
      if (!pending) {
        const chunks = yield* pull.pipe(Pull.catchDone(() => Effect.succeed(undefined)));
        if (!chunks) {
          ended = true;
          if (source.length !== undefined && total !== source.length)
            return yield* failure({
              _tag: "LengthMismatch",
              expected: source.length,
              actual: total,
            });
          checksum = hash.finish();
          break;
        }
        pending = chunks[0];
        offset = 0;
        if (!(pending instanceof Uint8Array))
          return yield* failure({ _tag: "InvalidInput", field: "source.chunk" });
        // A tiny subarray can keep a huge backing allocation alive through a retry.
        if (pending.buffer.byteLength > options.maximumChunkBytes)
          return yield* failure({ _tag: "LimitExceeded", limit: options.maximumChunkBytes });
        if (pending.byteLength === 0) {
          pending = undefined;
          continue;
        }
      }

      const count = Math.min(buffer.length - used, pending.length - offset);
      if (total + count > options.maximumBytes)
        return yield* failure({ _tag: "LimitExceeded", limit: options.maximumBytes });
      if (source.length !== undefined && total + count > source.length)
        return yield* failure({
          _tag: "LengthMismatch",
          expected: source.length,
          actual: total + count,
        });
      const slice = pending.subarray(offset, offset + count);
      buffer.set(slice, used);
      hash.update(slice);
      used += count;
      total += count;
      offset += count;
      options.onRead(count);
      if (offset === pending.length) pending = undefined;
    }
    return used === 0 ? undefined : buffer.subarray(0, used);
  });

  return {
    next,
    count: () => total,
    checksum: () => checksum,
  };
});

export function sameChecksum(actual: Checksum, expected: Checksum): boolean {
  if (
    expected.scope !== "full-object" ||
    actual.scope !== "full-object" ||
    actual.algorithm !== expected.algorithm
  )
    return false;
  const left = actual.encoding === "hex" ? Hex.decode(actual.value) : Base64.decode(actual.value);
  const right =
    expected.encoding === "hex" ? Hex.decode(expected.value) : Base64.decode(expected.value);
  return (
    left._tag === "Success" &&
    right._tag === "Success" &&
    left.success.length === right.success.length &&
    left.success.every((byte, index) => byte === right.success[index])
  );
}
