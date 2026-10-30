import { Clock, Effect, Redacted, Schema, Semaphore } from "effect";
import { KeyValueStore } from "effect/persistence";

import { handleUpload } from "@vercel/blob/client";

import { storageError } from "../errors.js";
import type { ObjectMetadata } from "../object.js";
import { VercelStorage } from "../vercel.js";
import { validateKey } from "./mapping.js";

const Event = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("blob.generate-client-token"),
    payload: Schema.Struct({
      pathname: Schema.String,
      clientPayload: Schema.NullOr(Schema.String),
      multipart: Schema.Boolean,
    }),
  }),
  Schema.Struct({
    type: Schema.Literal("blob.upload-completed"),
    payload: Schema.Struct({
      blob: Schema.Struct({
        url: Schema.String,
        downloadUrl: Schema.String,
        pathname: Schema.String,
        contentType: Schema.String,
        contentDisposition: Schema.String,
        etag: Schema.String,
      }),
      tokenPayload: Schema.optionalKey(Schema.NullOr(Schema.String)),
    }),
  }),
]);
const Authorization = Schema.Struct({
  maximumSizeInBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  allowedContentTypes: Schema.optionalKey(
    Schema.Array(Schema.NonEmptyString).check(Schema.isMinLength(1)),
  ),
  allowOverwrite: Schema.optionalKey(Schema.Boolean),
  tokenPayload: Schema.optionalKey(Schema.String),
});
export interface VercelUploadHandlerOptions<E, R> {
  readonly namespace: string;
  readonly ownership: "exclusive";
  readonly callbackUrl: string;
  readonly expiresInSeconds?: number;
  readonly authorize: (input: {
    readonly request: Request;
    readonly key: string;
    readonly clientPayload: string | null;
    readonly multipart: boolean;
  }) => Effect.Effect<typeof Authorization.Type, E, R>;
  /** Use idempotencyKey in the application's transaction: a crash before receipt persistence can redeliver. */
  readonly onObserved: (event: {
    readonly idempotencyKey: string;
    readonly metadata: ObjectMetadata;
    readonly tokenPayload: string | null;
  }) => Effect.Effect<void, E, R>;
}

const readEvent = Effect.fnUntraced(function* (request: Request) {
  const bytes = yield* Effect.tryPromise({
    try: async () => {
      const reader = request.body?.getReader();
      if (!reader) throw new Error("Missing body");
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        while (true) {
          // oxlint-disable-next-line no-await-in-loop -- Request chunks must be read sequentially under the body limit.
          const next = await reader.read();
          if (next.done) break;
          size += next.value.length;
          if (size > 65536) throw new Error("Body limit");
          chunks.push(next.value);
        }
      } finally {
        await reader.cancel().catch(() => undefined);
        reader.releaseLock();
      }
      const joined = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) {
        joined.set(chunk, offset);
        offset += chunk.length;
      }
      return joined;
    },
    catch: () => storageError("upload.handler", "vercel", { _tag: "InvalidInput", field: "body" }),
  });
  // Preserve the parsed JSON property order: the SDK verifies JSON.stringify(body).
  const parsed = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
    new TextDecoder().decode(bytes),
  ).pipe(
    Effect.mapError(() =>
      storageError("upload.handler", "vercel", { _tag: "InvalidInput", field: "body" }),
    ),
  );
  yield* Schema.decodeUnknownEffect(Event)(parsed).pipe(
    Effect.mapError(() =>
      storageError("upload.handler", "vercel", { _tag: "InvalidInput", field: "body" }),
    ),
  );
  return parsed as typeof Event.Type;
});

/** Creates an HTTP runtime boundary. The owning Effect scope must outlive the returned handler. */
export const VercelUploadHandler = {
  make: Effect.fnUntraced(function* <E, R>(options: VercelUploadHandlerOptions<E, R>) {
    const storage = yield* VercelStorage;
    const receipts = yield* KeyValueStore.KeyValueStore;
    const context = yield* Effect.context<R>();
    const lock = yield* Semaphore.make(1);
    const callback = URL.parse(options.callbackUrl);
    const lifetime = options.expiresInSeconds ?? 3600;
    if (
      options.ownership !== "exclusive" ||
      !options.namespace ||
      !callback ||
      !["https:", "http:"].includes(callback.protocol) ||
      callback.username ||
      callback.password ||
      callback.hash ||
      !Number.isSafeInteger(lifetime) ||
      lifetime < 1 ||
      lifetime > 604800 ||
      storage.config.credential.mode !== "token"
    )
      return yield* storageError("upload.handler", "vercel", {
        _tag: "InvalidConfiguration",
        field: "options-or-token-authority",
      });

    const execute = Effect.runPromiseWith(context);
    const run: typeof execute = (program, runOptions) =>
      execute(Effect.scoped(program), runOptions);
    const authorizeGrant = Effect.fnUntraced(function* (
      request: Request,
      key: string,
      clientPayload: string | null,
      multipart: boolean,
    ) {
      yield* validateKey(key, "grant");
      const authorized = yield* options.authorize({ request, key, clientPayload, multipart }).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Authorization)),
        Effect.mapError(() => storageError("grant", "vercel", { _tag: "AccessDenied" })),
      );
      if (authorized.maximumSizeInBytes > storage.config.maxObjectBytes)
        return yield* storageError("grant", "vercel", {
          _tag: "LimitExceeded",
          limit: storage.config.maxObjectBytes,
        });
      const now = yield* Clock.currentTimeMillis;
      return {
        maximumSizeInBytes: authorized.maximumSizeInBytes,
        ...(authorized.allowedContentTypes
          ? { allowedContentTypes: [...authorized.allowedContentTypes] }
          : {}),
        ...(authorized.allowOverwrite === undefined
          ? {}
          : { allowOverwrite: authorized.allowOverwrite }),
        // Avoid the SDK's fallback to untrusted clientPayload when no server payload is supplied.
        tokenPayload: authorized.tokenPayload ?? "",
        addRandomSuffix: false,
        callbackUrl: callback.href,
        validUntil: now + lifetime * 1000,
      };
    });
    type Completion = Extract<typeof Event.Type, { type: "blob.upload-completed" }>["payload"];
    const observeCompletion = Effect.fnUntraced(function* ({ blob, tokenPayload }: Completion) {
      yield* validateKey(blob.pathname, "callback");
      const metadata = yield* storage.head({ key: blob.pathname });
      if (!metadata.etag || metadata.etag !== blob.etag)
        return yield* storageError("callback", "vercel", { _tag: "PreconditionFailed" });
      const idempotencyKey = JSON.stringify([storage.storeId, blob.pathname, blob.etag]);
      const receiptKey = `${options.namespace}:${encodeURIComponent(idempotencyKey)}`;
      if (yield* receipts.has(receiptKey)) return;

      yield* options.onObserved({ idempotencyKey, metadata, tokenPayload: tokenPayload ?? null });
      yield* receipts.set(receiptKey, "delivered");
    });
    const handle = Effect.fnUntraced(function* (request: Request) {
      if (request.method !== "POST")
        return new Response(null, { status: 405, headers: { allow: "POST" } });
      const body = yield* readEvent(request);
      const credential = storage.config.credential.token;
      const token = yield* Effect.isEffect(credential) ? credential : Effect.succeed(credential);
      if (
        !Redacted.isRedacted(token) ||
        Redacted.value(token).split("_")[3] !== storage.config.storeId
      )
        return yield* storageError("upload.handler", "vercel", { _tag: "AccessDenied" });

      let callbackFailed = false;
      const result = yield* Effect.tryPromise({
        try: (signal) =>
          handleUpload({
            request,
            body,
            token: Redacted.value(token),
            onBeforeGenerateToken: (key, clientPayload, multipart) =>
              run(authorizeGrant(request, key, clientPayload, multipart), { signal }),
            // The SDK authenticates the callback signature before invoking this effect.
            onUploadCompleted: (completion) =>
              run(
                lock
                  .withPermits(1)(observeCompletion(completion))
                  .pipe(
                    Effect.tapError(() =>
                      Effect.sync(() => {
                        callbackFailed = true;
                      }),
                    ),
                  ),
                { signal },
              ),
          }),
        catch: () =>
          storageError("upload.handler", "vercel", {
            _tag: callbackFailed ? "ProviderFailure" : "AccessDenied",
          }),
      });
      return Response.json(result);
    });
    return (request: Request): Promise<Response> =>
      run(
        handle(request).pipe(
          Effect.catch((error) =>
            Effect.succeed(
              Response.json(
                { error: error.reason._tag },
                {
                  status:
                    error.reason._tag === "InvalidInput"
                      ? 400
                      : error.reason._tag === "AccessDenied"
                        ? 403
                        : 500,
                },
              ),
            ),
          ),
        ),
        { signal: request.signal },
      );
  }),
};
