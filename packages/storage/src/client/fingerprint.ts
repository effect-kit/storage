import { Effect } from "effect";

import { sha256 } from "../runtime/portable-hash.js";
import { failUpload } from "../uploads/schema.js";
import type { Progress } from "./transport.js";
export const hashFile = Effect.fnUntraced(function* (file: Blob, onProgress?: Progress) {
  const hash = sha256.create();
  for (let offset = 0; offset < file.size; offset += 1024 ** 2) {
    const bytes = yield* Effect.tryPromise({
      try: () => file.slice(offset, offset + 1024 ** 2).arrayBuffer(),
      catch: () => failUpload("Unavailable"),
    });
    hash.update(new Uint8Array(bytes));
    if (onProgress)
      yield* Effect.try({
        try: () =>
          onProgress({
            phase: "hashing",
            bytesAccepted: 0,
            totalBytes: file.size,
            measurement: "indeterminate",
          }),
        catch: () => failUpload("Unavailable"),
      });
  }
  return hash.finish().value;
});
