import { Clock, Context, DateTime, Effect, Layer, Redacted, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import { StorageSigning } from "../../core/capabilities.js";
import type { GrantConstraint } from "../../core/capabilities.js";
import { storageError } from "../../core/errors.js";
import { ObjectKey } from "../../core/object.js";
import { signingRequest } from "../../internal/signing-request.js";
import { SupabaseStorage } from "./index.js";
import { objectPath, validateKey, validateRef } from "./mapping.js";
import { json, request } from "./transport.js";

export class SupabaseUploadGrants extends Context.Service<
  SupabaseUploadGrants,
  {
    readonly create: (input: { readonly key: string; readonly upsert?: boolean }) => Effect.Effect<
      {
        readonly method: "PUT";
        readonly url: Redacted.Redacted<string>;
        readonly token: Redacted.Redacted<string>;
        readonly headers: Redacted.Redacted<Readonly<Record<string, string>>>;
        readonly expiresAt: DateTime.Utc;
        readonly constraints: ReadonlyArray<GrantConstraint>;
      },
      ReturnType<typeof storageError>
    >;
  }
>()("@effect-kit/storage/supabase/SupabaseUploadGrants") {}

const grantUrl = Effect.fnUntraced(function* (base: string, path: string, prefix: string) {
  const url = URL.parse(base + path);
  const origin = new URL(base).origin;
  if (
    !url ||
    url.origin !== origin ||
    url.pathname !== new URL(`${base}/${prefix}`).pathname ||
    url.username ||
    url.password ||
    url.hash ||
    !url.searchParams.get("token")
  )
    return yield* storageError("sign", "supabase", { _tag: "ProviderFailure" });
  return url;
});
const tokenExpiry = Effect.fnUntraced(function* (url: URL) {
  const token = url.searchParams.get("token") ?? "";
  const payload = yield* Effect.fromResult(Base64Url.decodeString(token.split(".")[1] ?? "")).pipe(
    Effect.mapError(() => storageError("sign", "supabase", { _tag: "ProviderFailure" })),
  );
  const claims = yield* Schema.decodeUnknownEffect(
    Schema.fromJsonString(
      Schema.Struct({ exp: Schema.Natural.check(Schema.isLessThanOrEqualTo(8640000000000)) }),
    ),
  )(payload).pipe(
    Effect.mapError(() => storageError("sign", "supabase", { _tag: "ProviderFailure" })),
  );
  if (claims.exp * 1000 <= (yield* Clock.currentTimeMillis))
    return yield* storageError("sign", "supabase", { _tag: "Expired" });
  return DateTime.makeUnsafe(claims.exp * 1000);
});
export const SupabaseSigning = {
  layer() {
    return Layer.effectContext(
      Effect.gen(function* () {
        const storage = yield* SupabaseStorage;
        const sign: StorageSigning["Service"]["sign"] = Effect.fnUntraced(function* (input) {
          yield* signingRequest(input, "supabase");
          yield* validateRef(storage, input.ref, "sign");
          if (/[?#%\\]/u.test(input.ref.key))
            return yield* storageError(
              "sign",
              "supabase",
              { _tag: "Unsupported", capability: "reserved-signing-key" },
              "not-started",
            );
          if (input.method !== "GET")
            return yield* storageError(
              "sign",
              "supabase",
              { _tag: "Unsupported", capability: "native-signing-method" },
              "not-started",
            );
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const response = yield* request(
                storage,
                "sign",
                "POST",
                `/object/sign/${objectPath(storage, input.ref.key)}`,
                { json: { expiresIn: input.expiresInSeconds } },
              );
              const body = yield* json(
                response,
                Schema.Struct({ signedURL: Schema.String }),
                "sign",
              );
              const url = yield* grantUrl(
                storage.config.baseUrl,
                body.signedURL,
                `object/sign/${objectPath(storage, input.ref.key)}`,
              );
              return {
                method: "GET" as const,
                url: Redacted.make(url.href),
                headers: Redacted.make({}),
                ref: input.ref,
                expiresAt: yield* tokenExpiry(url),
                constraints: [],
              };
            }),
          );
        });
        const create: SupabaseUploadGrants["Service"]["create"] = Effect.fnUntraced(
          function* (input) {
            yield* Schema.decodeUnknownEffect(
              Schema.Struct({ key: ObjectKey, upsert: Schema.optional(Schema.Boolean) }),
            )(input).pipe(
              Effect.mapError(() =>
                storageError(
                  "grant",
                  "supabase",
                  { _tag: "InvalidInput", field: "request" },
                  "not-started",
                ),
              ),
            );
            yield* validateKey(input.key, "grant");
            if (/[?#%\\]/u.test(input.key))
              return yield* storageError(
                "grant",
                "supabase",
                { _tag: "Unsupported", capability: "reserved-signing-key" },
                "not-started",
              );
            return yield* Effect.scoped(
              Effect.gen(function* () {
                const response = yield* request(
                  storage,
                  "grant",
                  "POST",
                  `/object/upload/sign/${objectPath(storage, input.key)}`,
                  { json: {}, headers: { "x-upsert": String(input.upsert ?? false) } },
                );
                const body = yield* json(response, Schema.Struct({ url: Schema.String }), "grant");
                const url = yield* grantUrl(
                  storage.config.baseUrl,
                  body.url,
                  `object/upload/sign/${objectPath(storage, input.key)}`,
                );
                return {
                  method: "PUT" as const,
                  url: Redacted.make(url.href),
                  token: Redacted.make(url.searchParams.get("token") ?? ""),
                  headers: Redacted.make({}),
                  expiresAt: yield* tokenExpiry(url),
                  constraints: [
                    {
                      kind: "upsert",
                      value: String(input.upsert ?? false),
                      enforcement: "provider" as const,
                    },
                  ],
                };
              }),
            );
          },
        );
        return Context.make(StorageSigning, { sign }).pipe(
          Context.add(SupabaseUploadGrants, { create }),
        );
      }),
    );
  },
};
