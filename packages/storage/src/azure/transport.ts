import { Effect } from "effect";

import { providerError } from "../internal/node/provider-errors.js";
import type { AzureConnection } from "./config.js";

export const request = <A>(
  connection: AzureConnection,
  operation: string,
  mutation: boolean,
  call: (signal: AbortSignal) => Promise<A>,
) =>
  Effect.tryPromise({
    try: call,
    catch: (error) => providerError("azure", operation, mutation, error),
  }).pipe(
    Effect.timeoutOrElse({
      duration: connection.config.requestTimeoutMillis,
      orElse: () => Effect.fail(providerError("azure", operation, mutation, undefined)),
    }),
  );
