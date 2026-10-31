import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";

import { storageError } from "../../core/errors.js";
import { controlEndpoint, controlRequest } from "../../internal/http/control-plane.js";
import { decodeResponse } from "../../internal/provider-response.js";
import { decodeRequest } from "../../internal/request-validation.js";

const Native = Schema.Record(Schema.String, Schema.Unknown);
const Name = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/u));
const Rule = Schema.Struct({
  id: Schema.NonEmptyString,
  enabled: Schema.Boolean,
  prefix: Schema.optional(Schema.String),
  condition: Schema.Union([
    Schema.Struct({ type: Schema.Literal("Age"), maxAgeSeconds: Schema.Natural }),
    Schema.Struct({
      type: Schema.Literal("Date"),
      date: Schema.String.check(Schema.makeFilter((value) => Number.isFinite(Date.parse(value)))),
    }),
    Schema.Struct({ type: Schema.Literal("Indefinite") }),
  ]),
});
export interface R2AdminConfig {
  readonly accountId: string;
  readonly token: Redacted.Redacted<string>;
  readonly endpoint?: string;
  readonly jurisdiction?: "default" | "eu" | "fedramp";
}
const bucket = (name: string) =>
  decodeRequest(Name, name, "bucket", "r2").pipe(Effect.map(encodeURIComponent));

const make = Effect.fnUntraced(function* (settings: R2AdminConfig) {
  const config = yield* decodeRequest(
    Schema.Struct({
      accountId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/u)),
      token: Schema.Redacted(Schema.NonEmptyString),
      endpoint: Schema.optional(Schema.String),
      jurisdiction: Schema.optional(Schema.Literals(["default", "eu", "fedramp"])),
    }),
    settings,
    "makeAdministration",
    "r2",
  );
  const endpoint = yield* controlEndpoint(config.endpoint ?? "https://api.cloudflare.com", "r2");
  const http = yield* HttpClient.HttpClient;
  const root = `${endpoint}/client/v4/accounts/${config.accountId}/r2/buckets`;
  const call = Effect.fnUntraced(function* (
    operation: string,
    method: "GET" | "POST" | "PUT" | "DELETE",
    path: string,
    body?: unknown,
    notification = false,
  ) {
    const envelope = yield* controlRequest(
      http,
      "r2",
      operation,
      notification
        ? `${endpoint}/client/v4/accounts/${config.accountId}/event_notifications/r2${path}`
        : root + path,
      method,
      {
        authorization: `Bearer ${Redacted.value(config.token)}`,
        ...(config.jurisdiction && config.jurisdiction !== "default"
          ? { "cf-r2-jurisdiction": config.jurisdiction }
          : {}),
      },
      body,
    );
    const value = yield* decodeResponse(
      Schema.Struct({ success: Schema.Boolean, result: Schema.optional(Schema.Unknown) }),
      envelope,
      operation,
      "r2",
    );
    if (!value.success)
      return yield* storageError(
        operation,
        "r2",
        { _tag: "ProviderFailure" },
        method === "GET" ? "not-applicable" : "unknown",
      );
    return value.result;
  });
  return {
    list: () => call("listBuckets", "GET", ""),
    head: (input: { readonly bucket: string }) =>
      bucket(input.bucket).pipe(Effect.flatMap((name) => call("headBucket", "GET", `/${name}`))),
    create: Effect.fnUntraced(function* (input: {
      readonly bucket: string;
      readonly locationHint?: string;
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({ bucket: Name, locationHint: Schema.optional(Schema.NonEmptyString) }),
        input,
        "createBucket",
        "r2",
        true,
      );
      return yield* call("createBucket", "POST", "", {
        name: value.bucket,
        ...(value.locationHint ? { locationHint: value.locationHint } : {}),
      });
    }),
    deleteEmpty: (input: { readonly bucket: string }) =>
      bucket(input.bucket).pipe(
        Effect.flatMap((name) => call("deleteBucket", "DELETE", `/${name}`)),
      ),
    notifications: {
      get: (input: { readonly bucket: string }) =>
        bucket(input.bucket).pipe(
          Effect.flatMap((name) =>
            call("getNotifications", "GET", `/${name}/configuration`, undefined, true),
          ),
        ),
      createRules: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly queueId: string;
        readonly rules: ReadonlyArray<{
          readonly actions: ReadonlyArray<
            | "PutObject"
            | "CopyObject"
            | "DeleteObject"
            | "CompleteMultipartUpload"
            | "LifecycleDeletion"
          >;
          readonly prefix?: string;
          readonly suffix?: string;
          readonly description?: string;
        }>;
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({
            bucket: Name,
            queueId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/u)),
            rules: Schema.Array(
              Schema.Struct({
                actions: Schema.Array(
                  Schema.Literals([
                    "PutObject",
                    "CopyObject",
                    "DeleteObject",
                    "CompleteMultipartUpload",
                    "LifecycleDeletion",
                  ]),
                ).check(Schema.isMinLength(1)),
                prefix: Schema.optional(Schema.String),
                suffix: Schema.optional(Schema.String),
                description: Schema.optional(Schema.String),
              }),
            ),
          }),
          input,
          "createNotificationRules",
          "r2",
          true,
        );
        return yield* call(
          "createNotificationRules",
          "PUT",
          `/${encodeURIComponent(value.bucket)}/configuration/queues/${value.queueId}`,
          { rules: value.rules },
          true,
        );
      }),
    },
    configuration: {
      get: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly section: "cors" | "lifecycle" | "locks" | "domains/managed";
      }) {
        const name = yield* bucket(input.bucket);
        const section = yield* decodeRequest(
          Schema.Literals(["cors", "lifecycle", "locks", "domains/managed"]),
          input.section,
          "getConfiguration",
          "r2",
        );
        return yield* call("getConfiguration", "GET", `/${name}/${section}`);
      }),
      setCors: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly rules: ReadonlyArray<Record<string, unknown>>;
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({ bucket: Name, rules: Schema.Array(Native) }),
          input,
          "setCors",
          "r2",
          true,
        );
        return yield* call("setCors", "PUT", `/${encodeURIComponent(value.bucket)}/cors`, {
          rules: value.rules,
        });
      }),
      setLifecycle: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly rules: ReadonlyArray<Record<string, unknown>>;
        readonly intent: "change-storage-protection";
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({
            bucket: Name,
            rules: Schema.Array(Native),
            intent: Schema.Literal("change-storage-protection"),
          }),
          input,
          "setLifecycle",
          "r2",
          true,
        );
        return yield* call(
          "setLifecycle",
          "PUT",
          `/${encodeURIComponent(value.bucket)}/lifecycle`,
          { rules: value.rules },
        );
      }),
      setLocks: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly rules: ReadonlyArray<typeof Rule.Type>;
        readonly intent: "change-bucket-locks";
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({
            bucket: Name,
            rules: Schema.Array(Rule),
            intent: Schema.Literal("change-bucket-locks"),
          }),
          input,
          "setLocks",
          "r2",
          true,
        );
        return yield* call("setLocks", "PUT", `/${encodeURIComponent(value.bucket)}/locks`, {
          rules: value.rules,
        });
      }),
    },
  };
});
export class R2Administration extends Context.Service<
  R2Administration,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/r2/R2Administration") {
  static readonly make = make;
  static layerHttp(config: R2AdminConfig) {
    return Layer.effect(R2Administration, make(config));
  }
  static layer(config: R2AdminConfig) {
    return R2Administration.layerHttp(config).pipe(Layer.provide(FetchHttpClient.layer));
  }
}
