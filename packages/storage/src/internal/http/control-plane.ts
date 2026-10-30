import { Effect, Stream } from "effect";
import { FetchHttpClient, HttpClientRequest } from "effect/http";
import type { HttpClient, HttpMethod } from "effect/http";

import { storageError } from "../../errors.js";
import { providerError } from "../node/provider-errors.js";

export const controlEndpoint = (value: string, provider: string) =>
  Effect.try({
    try: () => {
      const url = new URL(value);
      if (
        url.username ||
        url.password ||
        url.search ||
        url.hash ||
        url.pathname !== "/" ||
        (url.protocol !== "https:" &&
          !(url.protocol === "http:" && ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)))
      )
        throw new Error("endpoint");
      return url.origin;
    },
    catch: () =>
      storageError("makeAdministration", provider, {
        _tag: "InvalidConfiguration",
        field: "endpoint",
      }),
  });
export const controlRequest = Effect.fnUntraced(function* (
  http: HttpClient.HttpClient,
  provider: string,
  operation: string,
  url: string,
  method: HttpMethod.HttpMethod,
  headers: Readonly<Record<string, string>>,
  body?: unknown,
) {
  const mutation = !["GET", "HEAD"].includes(method);
  return yield* Effect.scoped(
    Effect.gen(function* () {
      if (Object.values(headers).some((value) => /[\r\n]/u.test(value)))
        return yield* storageError(
          operation,
          provider,
          { _tag: "InvalidConfiguration", field: "headers" },
          "not-started",
        );
      let request = HttpClientRequest.make(method)(url).pipe(HttpClientRequest.setHeaders(headers));
      if (body !== undefined)
        request = yield* HttpClientRequest.bodyJson(request, body).pipe(
          Effect.mapError(() =>
            storageError(
              operation,
              provider,
              { _tag: "InvalidInput", field: "body" },
              "not-started",
            ),
          ),
        );
      const response = yield* http.execute(request).pipe(
        Effect.provideService(FetchHttpClient.RequestInit, {
          redirect: "error",
          cache: "no-store",
        }),
        Effect.mapError(() => providerError(provider, operation, mutation, {})),
      );
      if (response.status < 200 || response.status >= 300)
        return yield* providerError(provider, operation, mutation, { statusCode: response.status });
      let total = 0;
      const chunks = yield* Stream.runCollect(
        response.stream.pipe(
          Stream.mapError(() => providerError(provider, operation, mutation, {})),
          Stream.mapEffect((chunk) => {
            total += chunk.byteLength;
            return total > 4 * 1024 ** 2
              ? Effect.fail(
                  storageError(
                    operation,
                    provider,
                    { _tag: "LimitExceeded", limit: 4 * 1024 ** 2 },
                    mutation ? "unknown" : "not-applicable",
                  ),
                )
              : Effect.succeed(chunk);
          }),
        ),
      );
      if (!total) return undefined;
      const bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      return yield* Effect.try({
        try: () => JSON.parse(new TextDecoder().decode(bytes)) as unknown,
        catch: () =>
          storageError(
            operation,
            provider,
            { _tag: "ProviderFailure", code: "InvalidJsonResponse" },
            mutation ? "unknown" : "not-applicable",
          ),
      });
    }),
  ).pipe(
    Effect.timeoutOrElse({
      duration: "30 seconds",
      orElse: () =>
        Effect.fail(
          storageError(
            operation,
            provider,
            { _tag: "TransportFailure", code: "DeadlineExceeded" },
            mutation ? "unknown" : "not-applicable",
          ),
        ),
    }),
  );
});
