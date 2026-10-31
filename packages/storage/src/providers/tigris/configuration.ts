import { Effect, Redacted, Schema } from "effect";

import * as Tigris from "@tigrisdata/storage";

import { storageError } from "../../core/errors.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { call } from "./transport.js";

const Bucket = Schema.NonEmptyString;
const Strings = Schema.Union([Schema.String, Schema.Array(Schema.String)]);
const Cors = Schema.Struct({
  allowedOrigins: Strings,
  allowedMethods: Schema.optional(Strings),
  allowedHeaders: Schema.optional(Strings),
  exposeHeaders: Schema.optional(Strings),
  maxAge: Schema.optional(Schema.Natural),
});
const Tier = Schema.Literals(["STANDARD", "STANDARD_IA", "GLACIER", "GLACIER_IR"]);
const Lifecycle = Schema.Struct({
  id: Schema.optional(Schema.String),
  enabled: Schema.optional(Schema.Boolean),
  storageClass: Schema.optional(Schema.Literals(["STANDARD_IA", "GLACIER", "GLACIER_IR"])),
  days: Schema.optional(Schema.Natural),
  date: Schema.optional(Schema.String),
  expiration: Schema.optional(
    Schema.Struct({ days: Schema.optional(Schema.Natural), date: Schema.optional(Schema.String) }),
  ),
  filter: Schema.optional(Schema.Struct({ prefix: Schema.String })),
});
const Settings = Schema.Struct({
  access: Schema.optional(Schema.Literals(["public", "private"])),
  allowObjectAcl: Schema.optional(Schema.Boolean),
  defaultTier: Schema.optional(Tier),
  disableDirectoryListing: Schema.optional(Schema.Boolean),
  cacheControl: Schema.optional(Schema.String),
  customDomain: Schema.optional(Schema.String),
  enableAdditionalHeaders: Schema.optional(Schema.Boolean),
  softDelete: Schema.optional(
    Schema.Union([
      Schema.Struct({
        enabled: Schema.Literal(true),
        retentionDays: Schema.Int.check(Schema.isGreaterThan(0)),
      }),
      Schema.Struct({ enabled: Schema.Literal(false) }),
    ]),
  ),
});

/** Native replacement operations have no provider CAS. Inspect with head before applying. */
export const configuration = (
  native: Effect.Effect<Tigris.TigrisConfig, ReturnType<typeof storageError>>,
) => ({
  update: Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly settings: typeof Settings.Type;
    readonly intent: "change-storage-protection";
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({
        bucket: Bucket,
        settings: Settings,
        intent: Schema.Literal("change-storage-protection"),
      }),
      input,
      "updateBucket",
      "tigris",
      true,
    );
    const config = yield* native;
    return yield* call("updateBucket", true, () =>
      Tigris.updateBucket(value.bucket, {
        ...(structuredClone(value.settings) as Omit<Tigris.UpdateBucketOptions, "config">),
        config,
      }),
    );
  }),
  setCors: Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly rules: ReadonlyArray<typeof Cors.Type>;
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({ bucket: Bucket, rules: Schema.Array(Cors) }),
      input,
      "setCors",
      "tigris",
      true,
    );
    const config = yield* native;
    return yield* call("setCors", true, () =>
      Tigris.setBucketCors(value.bucket, {
        rules: structuredClone(value.rules) as Tigris.BucketCorsRule[],
        override: true,
        config,
      }),
    );
  }),
  setLifecycle: Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly rules: ReadonlyArray<typeof Lifecycle.Type>;
    readonly intent: "change-storage-protection";
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({
        bucket: Bucket,
        rules: Schema.Array(Lifecycle),
        intent: Schema.Literal("change-storage-protection"),
      }),
      input,
      "setLifecycle",
      "tigris",
      true,
    );
    const config = yield* native;
    return yield* call("setLifecycle", true, () =>
      Tigris.setBucketLifecycle(value.bucket, {
        lifecycleRules: structuredClone(value.rules) as Tigris.BucketLifecycleRule[],
        config,
      }),
    );
  }),
  setNotifications: Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly enabled: boolean;
    readonly url?: string;
    readonly filter?: string;
    readonly token?: Redacted.Redacted<string>;
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({
        bucket: Bucket,
        enabled: Schema.Boolean,
        url: Schema.optional(Schema.String),
        filter: Schema.optional(Schema.String),
        token: Schema.optional(Schema.Redacted(Schema.NonEmptyString)),
      }),
      input,
      "setNotifications",
      "tigris",
      true,
    );
    const url = value.url ? URL.parse(value.url) : undefined;
    if (
      value.enabled &&
      (!url || url.protocol !== "https:" || url.username || url.password || !value.token)
    )
      return yield* storageError(
        "setNotifications",
        "tigris",
        { _tag: "InvalidInput", field: "authenticated-https-webhook" },
        "not-started",
      );
    const config = yield* native;
    return yield* call("setNotifications", true, () =>
      Tigris.setBucketNotifications(value.bucket, {
        override: true,
        notificationConfig: {
          enabled: value.enabled,
          ...(value.url ? { url: value.url } : {}),
          ...(value.filter ? { filter: value.filter } : {}),
          ...(value.token ? { auth: { token: Redacted.value(value.token) } } : {}),
        },
        config,
      }),
    );
  }),
});
