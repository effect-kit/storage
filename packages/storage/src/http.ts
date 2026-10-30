import { Effect, Layer, Redacted, Schema, Stream } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import type { HttpServerRequest } from "effect/http";

import { StorageEventInbox } from "./events.js";
import type { StorageEvent } from "./events.js";
import { privateDownload } from "./http/download.js";
import type { ObjectRef } from "./object.js";
import { Storage } from "./storage.js";
import { Uploads } from "./uploads.js";
import {
  PartRequest,
  PartsRequest,
  UploadActor,
  UploadRequest,
  failUpload,
} from "./uploads/schema.js";
import type { UploadError } from "./uploads/schema.js";
export { makeStorageUploadApi } from "./http/api.js";
export interface StorageHttpOptions<R = never> {
  readonly prefix?: `/${string}`;
  readonly allowedOrigins: ReadonlyArray<string>;
  readonly authentication: "cookie" | "bearer";
  readonly authenticate: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<UploadActor, UploadError, R>;
  readonly resolveDownload?: (input: {
    readonly actor: UploadActor;
    readonly fileId: string;
  }) => Effect.Effect<{ readonly ref: ObjectRef; readonly filename: string }, UploadError, R>;
}
const decode = <S extends Schema.Constraint>(schema: S, raw: unknown) =>
  Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(raw).pipe(
    Effect.mapError(() => failUpload("BadRequest")),
  );
const jsonBody = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  request: HttpServerRequest.HttpServerRequest,
  schema: S,
) {
  if (!request.headers["content-type"]?.toLowerCase().startsWith("application/json"))
    return yield* failUpload("BadRequest");
  let total = 0;
  const chunks = yield* Stream.runCollect(
    request.stream.pipe(
      Stream.mapEffect((bytes) => {
        total += bytes.length;
        return total > 256 * 1024 ? Effect.fail(failUpload("BadRequest")) : Effect.succeed(bytes);
      }),
      Stream.mapError(() => failUpload("BadRequest")),
    ),
  );
  const joined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.length;
  }
  const value = yield* Effect.try({
    try: () => JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(joined)) as unknown,
    catch: () => failUpload("BadRequest"),
  });
  return yield* decode(schema, value);
});
const statuses = {
  BadRequest: 400,
  Unauthorized: 401,
  Forbidden: 403,
  NotFound: 404,
  Conflict: 409,
  Expired: 410,
  Unsupported: 422,
  VerificationFailed: 422,
  Unavailable: 503,
} as const;
export const StorageHttp = {
  callbacks<R>(options: {
    readonly path: `/${string}`;
    readonly authenticate: (
      request: HttpServerRequest.HttpServerRequest,
    ) => Effect.Effect<ReadonlyArray<StorageEvent>, UploadError, R>;
    readonly resolve: (
      event: StorageEvent,
    ) => Effect.Effect<
      { readonly actor: UploadActor; readonly uploadId: string } | undefined,
      UploadError,
      R
    >;
  }) {
    return Layer.unwrap(
      Effect.gen(function* () {
        const uploads = yield* Uploads;
        const inbox = yield* StorageEventInbox;
        return HttpRouter.add("POST", options.path, (request) =>
          Effect.gen(function* () {
            const events = yield* options.authenticate(request);
            if (events.length > 1000) return yield* failUpload("BadRequest");
            yield* Effect.forEach(events, (event) =>
              inbox.process(event, () =>
                Effect.gen(function* () {
                  const binding = yield* options.resolve(event);
                  if (binding) yield* uploads.observe({ ...binding, event });
                }),
              ),
            );
            return HttpServerResponse.empty({
              status: 204,
              headers: { "cache-control": "no-store" },
            });
          }).pipe(
            Effect.catch(() =>
              Effect.succeed(
                HttpServerResponse.empty({ status: 503, headers: { "cache-control": "no-store" } }),
              ),
            ),
          ),
        );
      }),
    );
  },
  routes<R>(options: StorageHttpOptions<R>) {
    const prefix = options.prefix ?? "/storage";
    const setup = Effect.gen(function* () {
      if (
        !/^\/[A-Za-z0-9/_-]+$/u.test(prefix) ||
        prefix.endsWith("/") ||
        options.allowedOrigins.some((origin) => {
          const url = URL.parse(origin);
          return !url || url.origin !== origin;
        })
      )
        return yield* failUpload("BadRequest");
      const uploads = yield* Uploads;
      const storage = yield* Storage;
      const handler =
        (
          action:
            | "create"
            | "status"
            | "grant"
            | "part"
            | "complete"
            | "reconcile"
            | "abort"
            | "content"
            | "form"
            | "download",
        ) =>
        (request: HttpServerRequest.HttpServerRequest) =>
          Effect.gen(function* () {
            const origin = request.headers.origin;
            const unsafe = !["GET", "HEAD", "OPTIONS"].includes(request.method);
            if (
              (origin && !options.allowedOrigins.includes(origin)) ||
              (unsafe && options.authentication === "cookie" && !origin)
            )
              return yield* failUpload("Forbidden");
            const actor = yield* options
              .authenticate(request)
              .pipe(Effect.flatMap((identity) => decode(UploadActor, identity)));
            const params = yield* HttpRouter.params;
            const target = { actor, uploadId: params.id ?? "" };
            let response: HttpServerResponse.HttpServerResponse;
            if (action === "download") {
              if (!options.resolveDownload) return yield* failUpload("NotFound");
              response = yield* privateDownload(
                storage,
                request,
                yield* options.resolveDownload({ actor, fileId: params.fileId ?? "" }),
              );
            } else if (action === "create") {
              const payload = yield* jsonBody(request, UploadRequest);
              response = yield* HttpServerResponse.json(
                yield* uploads.create({
                  actor,
                  idempotencyKey: payload.idempotencyKey,
                  input: payload.input,
                }),
              );
            } else if (action === "grant" || action === "part") {
              const value =
                action === "part"
                  ? yield* uploads.part({ ...target, ...(yield* jsonBody(request, PartRequest)) })
                  : yield* uploads.grant(target);
              response = yield* HttpServerResponse.json({
                ...value,
                grant: Redacted.value(value.grant),
              });
            } else if (action === "content") {
              response = yield* HttpServerResponse.json(
                yield* uploads.content({ ...target, body: request.stream }),
              );
            } else if (action === "form") {
              let count = 0;
              yield* Stream.runForEach(request.multipartStream, (part) =>
                Effect.gen(function* () {
                  if (part._tag !== "File" || part.key !== "file" || ++count > 1)
                    return yield* failUpload("BadRequest");
                  yield* uploads.content({ ...target, body: part.content });
                }),
              );
              if (count !== 1) return yield* failUpload("BadRequest");
              response = yield* HttpServerResponse.json(yield* uploads.status(target));
            } else if (action === "complete") {
              const payload = yield* jsonBody(
                request,
                Schema.Struct({ parts: Schema.optional(PartsRequest.fields.parts) }),
              );
              response = yield* HttpServerResponse.json(
                yield* uploads.complete({
                  ...target,
                  ...(payload.parts ? { parts: payload.parts } : {}),
                }),
              );
            } else response = yield* HttpServerResponse.json(yield* uploads[action](target));
            return response.pipe(
              HttpServerResponse.setHeaders({
                "cache-control": "private, no-store",
                vary: "Origin",
                ...(origin
                  ? {
                      "access-control-allow-origin": origin,
                      "access-control-allow-credentials": "true",
                      "access-control-expose-headers": "ETag, Content-Range, Content-Disposition",
                    }
                  : {}),
              }),
            );
          }).pipe(
            Effect.catch((error) => {
              const code =
                error &&
                typeof error === "object" &&
                "_tag" in error &&
                error._tag === "UploadError"
                  ? (error as UploadError).code
                  : "Unavailable";
              return HttpServerResponse.json(
                { _tag: "UploadError", code },
                {
                  status: statuses[code],
                  headers: {
                    "cache-control": "no-store",
                    ...(request.headers.origin &&
                    options.allowedOrigins.includes(request.headers.origin)
                      ? {
                          "access-control-allow-origin": request.headers.origin,
                          "access-control-allow-credentials": "true",
                          vary: "Origin",
                        }
                      : {}),
                  },
                },
              ).pipe(Effect.orDie);
            }),
          );
      return Layer.mergeAll(
        HttpRouter.add("POST", `${prefix}/uploads`, handler("create")),
        HttpRouter.add("GET", `${prefix}/uploads/:id`, handler("status")),
        HttpRouter.add("POST", `${prefix}/uploads/:id/grants`, handler("grant")),
        HttpRouter.add("POST", `${prefix}/uploads/:id/parts/sign`, handler("part")),
        HttpRouter.add("POST", `${prefix}/uploads/:id/complete`, handler("complete")),
        HttpRouter.add("POST", `${prefix}/uploads/:id/reconcile`, handler("reconcile")),
        HttpRouter.add("DELETE", `${prefix}/uploads/:id`, handler("abort")),
        HttpRouter.add("PUT", `${prefix}/uploads/:id/content`, handler("content")),
        HttpRouter.add("POST", `${prefix}/uploads/:id/content`, handler("form")),
        HttpRouter.add("GET", `${prefix}/files/:fileId`, handler("download")),
        HttpRouter.add("OPTIONS", `${prefix}/*`, (request) =>
          Effect.succeed(
            options.allowedOrigins.includes(request.headers.origin ?? "")
              ? HttpServerResponse.empty({
                  status: 204,
                  headers: {
                    "access-control-allow-origin": request.headers.origin ?? "",
                    "access-control-allow-credentials": "true",
                    "access-control-allow-methods": "GET, HEAD, POST, PUT, DELETE, OPTIONS",
                    "access-control-allow-headers":
                      "Content-Type, Authorization, Range, If-Match, If-None-Match, If-Range",
                    vary: "Origin",
                  },
                })
              : HttpServerResponse.empty({ status: 403 }),
          ),
        ),
      );
    });
    return Layer.unwrap(setup);
  },
};
