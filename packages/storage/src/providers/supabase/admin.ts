import type { Redacted } from "effect";
import { Context, Effect, Layer, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";

import { decodeRequest } from "../../internal/request-validation.js";
import { resolveConfig } from "./config.js";
import { json, request } from "./transport.js";

export interface SupabaseAdminConfig {
  readonly url: string;
  readonly token: Redacted.Redacted<string>;
  readonly apiKey?: Redacted.Redacted<string>;
}
const Name = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._-]+$/u));
const Settings = Schema.Struct({
  bucket: Name,
  public: Schema.Boolean,
  fileSizeLimit: Schema.optional(Schema.Natural),
  allowedMimeTypes: Schema.optional(Schema.Array(Schema.NonEmptyString)),
});
const Native = Schema.Record(Schema.String, Schema.Unknown);
const make = Effect.fnUntraced(function* (settings: SupabaseAdminConfig) {
  const config = yield* resolveConfig({
    url: settings.url,
    bucket: "administration",
    storeId: "supabase-administration",
    auth: {
      mode: "service",
      token: settings.token,
      ...(settings.apiKey ? { apiKey: settings.apiKey } : {}),
    },
  });
  const connection = { config, http: yield* HttpClient.HttpClient };
  return {
    list: () =>
      Effect.scoped(
        request(connection, "listBuckets", "GET", "/bucket").pipe(
          Effect.flatMap((response) => json(response, Schema.Array(Native), "listBuckets")),
        ),
      ),
    head: Effect.fnUntraced(function* (input: { readonly bucket: string }) {
      const value = yield* decodeRequest(
        Schema.Struct({ bucket: Name }),
        input,
        "headBucket",
        "supabase",
      );
      return yield* Effect.scoped(
        request(
          connection,
          "headBucket",
          "GET",
          `/bucket/${encodeURIComponent(value.bucket)}`,
        ).pipe(Effect.flatMap((response) => json(response, Native, "headBucket"))),
      );
    }),
    create: Effect.fnUntraced(function* (input: typeof Settings.Type) {
      const value = yield* decodeRequest(Settings, input, "createBucket", "supabase", true);
      return yield* Effect.scoped(
        request(connection, "createBucket", "POST", "/bucket", {
          json: {
            id: value.bucket,
            name: value.bucket,
            public: value.public,
            file_size_limit: value.fileSizeLimit,
            allowed_mime_types: value.allowedMimeTypes,
          },
        }).pipe(Effect.flatMap((response) => json(response, Native, "createBucket"))),
      );
    }),
    update: Effect.fnUntraced(function* (
      input: typeof Settings.Type & { readonly intent: "change-bucket-access-and-limits" },
    ) {
      const value = yield* decodeRequest(
        Schema.Struct({
          ...Settings.fields,
          intent: Schema.Literal("change-bucket-access-and-limits"),
        }),
        input,
        "updateBucket",
        "supabase",
        true,
      );
      return yield* Effect.scoped(
        request(connection, "updateBucket", "PUT", `/bucket/${encodeURIComponent(value.bucket)}`, {
          json: {
            public: value.public,
            file_size_limit: value.fileSizeLimit,
            allowed_mime_types: value.allowedMimeTypes,
          },
        }).pipe(Effect.flatMap((response) => json(response, Native, "updateBucket"))),
      );
    }),
    deleteEmpty: Effect.fnUntraced(function* (input: { readonly bucket: string }) {
      const value = yield* decodeRequest(
        Schema.Struct({ bucket: Name }),
        input,
        "deleteBucket",
        "supabase",
        true,
      );
      yield* Effect.scoped(
        request(
          connection,
          "deleteBucket",
          "DELETE",
          `/bucket/${encodeURIComponent(value.bucket)}`,
        ).pipe(Effect.asVoid),
      );
    }),
  };
});
export class SupabaseAdministration extends Context.Service<
  SupabaseAdministration,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/supabase/SupabaseAdministration") {
  static readonly make = make;
  static layerHttp(config: SupabaseAdminConfig) {
    return Layer.effect(SupabaseAdministration, make(config));
  }
  static layer(config: SupabaseAdminConfig) {
    return SupabaseAdministration.layerHttp(config).pipe(Layer.provide(FetchHttpClient.layer));
  }
}
