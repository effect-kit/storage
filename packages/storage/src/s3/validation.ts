import { validateHeaderValue } from "node:http";

import { Effect, Schema } from "effect";

import { storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { ObjectHeaders, ObjectKey } from "../object.js";

export const S3Key = ObjectKey.check(
  Schema.makeFilter<string>((key) => new TextEncoder().encode(key).length <= 1024, {
    expected: "an S3 key of at most 1024 UTF-8 bytes",
  }),
);

export const validateHeaders = Effect.fnUntraced(function* (
  input: ObjectHeaders,
  operation: string,
) {
  const value = yield* decodeRequest(ObjectHeaders, input, operation, "s3", true);
  const names = Object.keys(value.customMetadata ?? {}).map((name) => name.toLowerCase());
  const bytes = Object.entries(value.customMetadata ?? {}).reduce(
    (total, [name, content]) => total + Buffer.byteLength(name) + Buffer.byteLength(content),
    0,
  );
  if (new Set(names).size !== names.length || bytes > 2048)
    return yield* storageError(
      operation,
      "s3",
      { _tag: "InvalidInput", field: "metadata" },
      "not-started",
    );
  yield* Effect.try({
    try: () => {
      for (const [name, content] of Object.entries(value)) {
        if (typeof content === "string") validateHeaderValue(name, content);
      }
      for (const [name, content] of Object.entries(value.customMetadata ?? {}))
        validateHeaderValue(name, content);
    },
    catch: () =>
      storageError(operation, "s3", { _tag: "InvalidInput", field: "headers" }, "not-started"),
  });
  return value;
});
