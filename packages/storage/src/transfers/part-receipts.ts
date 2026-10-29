import { Effect, Schema } from "effect";

import type { UploadedPart } from "../contracts/multipart.js";
import { storageError } from "../errors.js";
import { Checksum, ObjectSize } from "../object.js";

const Receipt = Schema.Struct({
  partNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  etag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  size: Schema.optional(ObjectSize),
  checksums: Schema.optional(
    Schema.Array(
      Schema.Struct({
        ...Checksum.fields,
        algorithm: Schema.String.check(Schema.isMaxLength(32)),
        value: Schema.String.check(Schema.isMaxLength(128)),
      }),
    ).check(Schema.isMaxLength(8)),
  ),
});

/** Bounds identities retained until completion and the subsequent protocol serialization. */
export const partReceipt = Effect.fnUntraced(function* (
  input: UploadedPart,
  partNumber: number,
  size: number,
  provider: string,
) {
  const receipt = yield* Schema.decodeUnknownEffect(Receipt)(input).pipe(
    Effect.mapError(() =>
      storageError("upload.part", provider, { _tag: "IntegrityMismatch" }, "not-started"),
    ),
  );
  if (receipt.partNumber !== partNumber)
    return yield* storageError(
      "upload.part",
      provider,
      { _tag: "IntegrityMismatch" },
      "not-started",
    );
  if (receipt.size !== undefined && receipt.size !== size)
    return yield* storageError(
      "upload.part",
      provider,
      { _tag: "LengthMismatch", expected: size, actual: receipt.size ?? 0 },
      "not-started",
    );
  return { ...receipt, size };
});
