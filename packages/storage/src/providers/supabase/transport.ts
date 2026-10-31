import { Effect, Option, Redacted, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import type { HttpMethod } from "effect/http";

import { storageError } from "../../core/errors.js";
import { providerError } from "../../internal/node/provider-errors.js";
import type { SupabaseConnection } from "./config.js";

export const request = Effect.fnUntraced(function* (
  connection: SupabaseConnection,
  operation: string,
  method: HttpMethod.HttpMethod,
  path: string,
  options: {
    readonly headers?: Readonly<Record<string, string>>;
    readonly json?: unknown;
    readonly body?: Uint8Array | ReadableStream<Uint8Array>;
    readonly accepted?: ReadonlyArray<number>;
  } = {},
) {
  const { config } = connection;
  const token = yield* Effect.isEffect(config.auth.token)
    ? config.auth.token
    : Effect.succeed(config.auth.token);
  if (
    !Redacted.isRedacted(token) ||
    !Redacted.value(token) ||
    /[\r\n]/u.test(Redacted.value(token))
  )
    return yield* storageError(operation, "supabase", { _tag: "AccessDenied" }, "not-started");
  let outgoing = HttpClientRequest.make(method)(
    path.startsWith("http") ? path : config.baseUrl + path,
  ).pipe(
    HttpClientRequest.setHeaders({
      authorization: `Bearer ${Redacted.value(token)}`,
      ...(config.auth.apiKey ? { apikey: Redacted.value(config.auth.apiKey) } : {}),
      ...options.headers,
    }),
  );
  if (options.json !== undefined)
    outgoing = HttpClientRequest.bodyJsonUnsafe(outgoing, options.json);
  if (options.body instanceof Uint8Array)
    outgoing = HttpClientRequest.bodyUint8Array(
      outgoing,
      options.body,
      options.headers?.["content-type"],
    );
  else if (options.body)
    outgoing = HttpClientRequest.bodyStream(
      outgoing,
      Stream.fromReadableStream({
        evaluate: () => options.body as ReadableStream<Uint8Array>,
        onError: () => storageError(operation, "supabase", { _tag: "TransportFailure" }, "unknown"),
      }),
      { contentType: options.headers?.["content-type"] },
    );
  const mutation = !["GET", "HEAD", "OPTIONS"].includes(method) && operation !== "list";
  const response = yield* connection.http.execute(outgoing).pipe(
    Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error", cache: "no-store" }),
    Effect.timeout(config.requestTimeoutMillis),
    Effect.mapError(() => providerError("supabase", operation, mutation, {})),
  );
  yield* Effect.addFinalizer(() =>
    Effect.promise(
      () =>
        Option.getOrUndefined(HttpClientResponse.toWeb(response))
          ?.body?.cancel()
          .catch(() => undefined) ?? Promise.resolve(),
    ),
  );
  if (
    (response.status < 200 || response.status >= 300) &&
    !options.accepted?.includes(response.status)
  ) {
    const fallback = providerError("supabase", operation, mutation, { code: response.status });
    if (!response.headers["content-type"]?.includes("application/json")) return yield* fallback;
    const parsed = yield* json(
      response,
      Schema.Struct({ code: Schema.optional(Schema.String) }),
      "error",
    ).pipe(Effect.result);
    if (parsed._tag === "Failure") return yield* fallback;
    const code = parsed.success.code;
    const reason =
      code === "ExpiredToken"
        ? { _tag: "Expired" as const }
        : response.status === 401 ||
            response.status === 403 ||
            code === "AccessDenied" ||
            code === "InvalidJWT"
          ? { _tag: "AccessDenied" as const }
          : code === "NoSuchKey" || code === "NoSuchBucket"
            ? { _tag: "NotFound" as const }
            : code === "ResourceAlreadyExists" ||
                code === "KeyAlreadyExists" ||
                code === "PreconditionFailed"
              ? { _tag: "PreconditionFailed" as const }
              : code === "InvalidRange"
                ? { _tag: "RangeNotSatisfiable" as const }
                : code === "FeatureNotEnabled" || code === "NotSupported"
                  ? { _tag: "Unsupported" as const, capability: "native-api" }
                  : undefined;
    return yield* reason
      ? storageError(operation, "supabase", reason, fallback.mutationOutcome)
      : fallback;
  }
  return response;
});

export const json = Effect.fnUntraced(function* <S extends Schema.Top>(
  response: HttpClientResponse.HttpClientResponse,
  schema: S,
  operation: string,
  mutation = false,
) {
  let size = 0;
  const chunks: Uint8Array[] = [];
  yield* Stream.runForEach(response.stream, (chunk) =>
    Effect.gen(function* () {
      size += chunk.length;
      if (size > (operation === "list" ? 4 * 1024 ** 2 : 65536))
        return yield* storageError(
          operation,
          "supabase",
          { _tag: "ProviderFailure" },
          mutation ? "unknown" : "not-applicable",
        );
      chunks.push(chunk);
    }),
  ).pipe(
    Effect.mapError(() =>
      storageError(
        operation,
        "supabase",
        { _tag: "ProviderFailure" },
        mutation ? "unknown" : "not-applicable",
      ),
    ),
  );
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(
    new TextDecoder().decode(bytes),
  ).pipe(
    Effect.mapError(() =>
      storageError(
        operation,
        "supabase",
        { _tag: "ProviderFailure" },
        mutation ? "unknown" : "not-applicable",
      ),
    ),
  );
});
