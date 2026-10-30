import { Effect, Redacted } from "effect";

import { storageError } from "../errors.js";
import type { VercelConnection } from "./config.js";
import { retryPolicy } from "./config.js";
import { providerError } from "./mapping.js";

export const call = Effect.fnUntraced(function* <A>(
  connection: VercelConnection,
  operation: string,
  mutation: boolean,
  execute: (options: {
    token?: string;
    oidcToken?: string;
    storeId: string;
    abortSignal: AbortSignal;
  }) => Promise<A>,
) {
  yield* retryPolicy();
  const currentEndpoint =
    process.env.VERCEL_BLOB_API_URL ?? process.env.NEXT_PUBLIC_VERCEL_BLOB_API_URL;
  if (
    (currentEndpoint ? URL.parse(currentEndpoint)?.origin : undefined) !==
    connection.config.apiOrigin
  )
    return yield* storageError(
      operation,
      "vercel",
      { _tag: "InvalidConfiguration", field: "changed-sdk-endpoint" },
      "not-started",
    );
  const credential = connection.config.credential;
  const token = yield* Effect.isEffect(credential.token)
    ? credential.token
    : Effect.succeed(credential.token);
  if (!Redacted.isRedacted(token) || !Redacted.value(token))
    return yield* storageError(operation, "vercel", { _tag: "AccessDenied" }, "not-started");
  if (
    credential.mode === "token" &&
    Redacted.value(token).split("_")[3] !== connection.config.storeId
  )
    return yield* storageError(
      operation,
      "vercel",
      { _tag: "InvalidConfiguration", field: "credential.storeId" },
      "not-started",
    );
  return yield* Effect.tryPromise({
    try: (signal) =>
      execute({
        [credential.mode === "token" ? "token" : "oidcToken"]: Redacted.value(token),
        storeId: connection.config.storeId,
        abortSignal: signal,
      }),
    catch: (error) => providerError(operation, mutation, error),
  }).pipe(
    Effect.timeout(connection.config.requestTimeoutMillis),
    Effect.mapError((error) =>
      error._tag === "StorageError" ? error : providerError(operation, mutation, error),
    ),
  );
});
