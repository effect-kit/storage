import { Effect, Schema } from "effect";

import { storageError } from "../errors.js";

/** Native protocols may add response fields; application request validation remains strict. */
export function decodeResponse<S extends Schema.Constraint>(
  schema: S,
  input: unknown,
  operation: string,
  provider: string,
  mutation = false,
) {
  return Schema.decodeUnknownEffect(schema)(input).pipe(
    Effect.mapError(() =>
      storageError(
        operation,
        provider,
        { _tag: "ProviderFailure", code: "InvalidResponse" },
        mutation ? "unknown" : "not-applicable",
      ),
    ),
  );
}
