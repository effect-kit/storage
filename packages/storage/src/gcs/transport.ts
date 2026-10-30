import { request as httpRequest } from "node:http";
import type { IncomingMessage } from "node:http";
import { request as httpsRequest } from "node:https";
import { Readable } from "node:stream";

import { Effect, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { storageError } from "../errors.js";
import { providerError } from "../internal/node/provider-errors.js";
import { customerEncryption } from "./config.js";
import type { GcsConnection } from "./config.js";

export interface Options {
  readonly uri: string;
  readonly method?: string;
  readonly qs?: Readonly<Record<string, string | number | undefined>>;
  readonly headers?: Readonly<Record<string, string>>;
  readonly json?: unknown;
  readonly body?: Uint8Array | Readable;
}
export interface JsonResponse {
  readonly status: number;
  readonly headers: Record<string, string>;
  readonly body: unknown;
}

/** SDK authorization/refresh plus a single abortable HTTP exchange. No File numeric IDs or hidden retries. */
const openResponse = (
  connection: GcsConnection,
  operation: string,
  mutation: boolean,
  options: Options,
) =>
  Effect.acquireRelease(
    Effect.interruptible(
      Effect.callback<IncomingMessage, ReturnType<typeof providerError>>((resume) => {
        let pending: ReturnType<typeof httpRequest> | undefined;
        let response: IncomingMessage | undefined;
        let cancelled = false;
        const preparation = connection.client.makeAuthenticatedRequest(
          { uri: options.uri, method: options.method ?? "GET", headers: { ...options.headers } },
          {
            onAuthenticated: (error, authenticated) => {
              if (cancelled) return;
              if (error || !authenticated) {
                resume(Effect.fail(providerError("gcs", operation, mutation, error)));
                return;
              }
              const url = new URL(options.uri);
              for (const [key, value] of Object.entries(options.qs ?? {}))
                if (value !== undefined) url.searchParams.set(key, String(value));
              const json =
                options.json !== undefined && typeof options.json !== "boolean"
                  ? Buffer.from(JSON.stringify(options.json))
                  : undefined;
              const body = json ?? options.body;
              pending = (url.protocol === "https:" ? httpsRequest : httpRequest)(
                url,
                {
                  method: options.method ?? "GET",
                  headers: {
                    ...Object.fromEntries(new Headers(authenticated.headers).entries()),
                    ...(/\/o(?:\/|$)/u.test(url.pathname)
                      ? customerEncryption(connection.config)
                      : {}),
                    ...(url.pathname.includes("/rewriteTo/")
                      ? customerEncryption(connection.config, true)
                      : {}),
                    ...options.headers,
                    ...(json
                      ? {
                          "Content-Type": "application/json",
                          "Content-Length": String(json.length),
                        }
                      : {}),
                  },
                },
                (incoming) => {
                  response = incoming;
                  incoming.on("error", () => {});
                  resume(Effect.succeed(incoming));
                },
              );
              pending.on("error", (failure) =>
                resume(Effect.fail(providerError("gcs", operation, mutation, failure))),
              );
              pending.setTimeout(connection.config.requestTimeoutMillis, () =>
                pending?.destroy(new Error("Storage request timeout")),
              );
              if (body instanceof Readable) {
                body.once("error", () => pending?.destroy());
                body.pipe(pending);
              } else pending.end(body);
            },
          },
        );
        return Effect.sync(() => {
          cancelled = true;
          if (preparation && "abort" in preparation) preparation.abort();
          pending?.destroy();
          response?.destroy();
          if (options.body instanceof Readable) options.body.destroy();
        });
      }),
    ),
    (response) =>
      Effect.sync(() => {
        response.destroy();
        if (options.body instanceof Readable) options.body.destroy();
      }),
  );

function headersOf(response: IncomingMessage) {
  return Object.fromEntries(
    Object.entries(response.headers).flatMap(([key, value]) =>
      value === undefined ? [] : [[key, Array.isArray(value) ? value.join(",") : value]],
    ),
  );
}
export const jsonRequest = Effect.fnUntraced(function* (
  connection: GcsConnection,
  operation: string,
  mutation: boolean,
  options: Options,
  acceptedStatuses: ReadonlyArray<number> = [],
) {
  if (options.json !== undefined && typeof options.json !== "boolean")
    yield* Effect.try({
      try: () => JSON.stringify(options.json),
      catch: () =>
        storageError(
          operation,
          "gcs",
          { _tag: "InvalidInput", field: "json" },
          mutation ? "not-started" : "not-applicable",
        ),
    });
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const response = yield* openResponse(connection, operation, mutation, options);
      const status = response.statusCode ?? 0;
      if (status === 304 && mutation)
        return yield* storageError(operation, "gcs", { _tag: "PreconditionFailed" }, "rejected");
      if ((status < 200 || status >= 300) && !acceptedStatuses.includes(status))
        return yield* providerError("gcs", operation, mutation, { code: status });
      const body = yield* Effect.tryPromise({
        try: async () => {
          const chunks: Buffer[] = [];
          let length = 0;
          for await (const chunk of response) {
            length += chunk.length;
            if (length > 4 * 1024 * 1024) throw new Error("Metadata limit");
            chunks.push(Buffer.from(chunk));
          }
          const text = Buffer.concat(chunks).toString("utf8");
          return !text
            ? undefined
            : response.headers["content-type"]?.includes("application/json") ||
                (options.json !== undefined && status !== 308)
              ? (JSON.parse(text) as unknown)
              : text;
        },
        catch: (error) => providerError("gcs", operation, mutation, error),
      });
      return { status, headers: headersOf(response), body };
    }),
  );
});
const mutationOperations = new Set([
  "put",
  "initiate",
  "write",
  "updateMetadata",
  "rewrite",
  "compose",
  "resumable",
]);
export const decodeJson = <S extends Schema.Top>(schema: S, body: unknown, operation: string) =>
  Schema.decodeUnknownEffect(schema)(body).pipe(
    Effect.mapError(() =>
      storageError(
        operation,
        "gcs",
        { _tag: "ProviderFailure" },
        mutationOperations.has(operation) ? "unknown" : "not-applicable",
      ),
    ),
  );

export const mediaRequest: (
  connection: GcsConnection,
  options: Options,
) => Effect.Effect<
  {
    status: number;
    headers: Record<string, string>;
    body: Stream.Stream<Uint8Array, ReturnType<typeof providerError>>;
  },
  ReturnType<typeof providerError>,
  Scope.Scope
> = Effect.fnUntraced(function* (connection: GcsConnection, options: Options) {
  const response = yield* openResponse(connection, "get", false, options);
  const status = response.statusCode ?? 0;
  if (status >= 400) return yield* providerError("gcs", "get", false, { code: status });
  return {
    status,
    headers: headersOf(response),
    body: Stream.fromAsyncIterable(response, (error) => providerError("gcs", "get", false, error)),
  };
});
