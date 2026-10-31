import { Effect } from "effect";

import { storageError } from "../../core/errors.js";
import { providerError } from "../../internal/node/provider-errors.js";
export const call = <A>(
  operation: string,
  mutation: boolean,
  run: () => Promise<{ data: A; error?: never } | { error: Error; data?: never }>,
) =>
  Effect.tryPromise({
    try: run,
    catch: (error) => providerError("tigris", operation, mutation, error),
  }).pipe(
    Effect.flatMap((result) =>
      result.error
        ? Effect.fail(providerError("tigris", operation, mutation, result.error))
        : Effect.succeed(result.data),
    ),
    Effect.timeoutOrElse({
      duration: "30 seconds",
      orElse: () =>
        Effect.fail(
          storageError(
            operation,
            "tigris",
            { _tag: "TransportFailure", code: "DeadlineExceeded" },
            mutation ? "unknown" : "not-applicable",
          ),
        ),
    }),
  );
