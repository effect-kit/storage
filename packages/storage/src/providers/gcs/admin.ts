import { Context, Effect, Layer, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { configurationState } from "../../lifecycle/configuration-state.js";
import { iam } from "./iam.js";
import { GcsStorage } from "./index.js";
import type { GcsConfig, GcsStorageService } from "./index.js";
import { jsonRequest, decodeJson } from "./transport.js";

const Bucket = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$/u));
const Native = Schema.Record(Schema.String, Schema.Unknown);
const Configuration = Schema.Struct({
  cors: Schema.optional(Schema.Array(Native)),
  lifecycle: Schema.optional(Native),
  versioning: Schema.optional(Schema.Struct({ enabled: Schema.Boolean })),
  encryption: Schema.optional(Native),
  retentionPolicy: Schema.optional(Native),
  softDeletePolicy: Schema.optional(Native),
  iamConfiguration: Schema.optional(Native),
  labels: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  logging: Schema.optional(Native),
  website: Schema.optional(Native),
  autoclass: Schema.optional(Native),
});
export type GcsBucketConfiguration = typeof Configuration.Type;
const makeService = (storage: GcsStorageService) => {
  const path = (bucket: string) =>
    `${storage.config.apiEndpoint}/storage/v1/b/${encodeURIComponent(bucket)}`;
  const get = Effect.fnUntraced(function* (input: { readonly bucket: string }) {
    const value = yield* decodeRequest(
      Schema.Struct({ bucket: Bucket }),
      input,
      "headBucket",
      "gcs",
    );
    const output = yield* jsonRequest(storage, "headBucket", false, {
      uri: path(value.bucket),
      json: true,
    });
    return yield* decodeJson(
      Schema.Struct({
        name: Schema.String,
        metageneration: Schema.String.check(Schema.isPattern(/^\d+$/u)),
        ...Configuration.fields,
      }),
      output.body,
      "headBucket",
    );
  });
  const plans = new WeakMap<
    object,
    { snapshot: string; bucket: string; metageneration: string; desired: GcsBucketConfiguration }
  >();
  return {
    head: get,
    iam: iam(storage),
    listPage: Effect.fnUntraced(function* (input: {
      readonly project: string;
      readonly cursor?: string;
      readonly limit?: number;
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          project: Schema.NonEmptyString,
          cursor: Schema.optional(Schema.String),
          limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
        }),
        input,
        "listBuckets",
        "gcs",
      );
      const output = yield* jsonRequest(storage, "listBuckets", false, {
        uri: `${storage.config.apiEndpoint}/storage/v1/b`,
        qs: { project: value.project, pageToken: value.cursor, maxResults: value.limit ?? 1000 },
        json: true,
      });
      return yield* decodeJson(
        Schema.Struct({
          items: Schema.optional(Schema.Array(Native)),
          nextPageToken: Schema.optional(Schema.String),
        }),
        output.body,
        "listBuckets",
      );
    }),
    create: Effect.fnUntraced(function* (input: {
      readonly bucket: string;
      readonly project: string;
      readonly location: string;
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          bucket: Bucket,
          project: Schema.NonEmptyString,
          location: Schema.NonEmptyString,
        }),
        input,
        "createBucket",
        "gcs",
        true,
      );
      const output = yield* jsonRequest(storage, "createBucket", true, {
        uri: `${storage.config.apiEndpoint}/storage/v1/b`,
        method: "POST",
        qs: { project: value.project },
        json: { name: value.bucket, location: value.location },
      });
      return yield* decodeJson(Native, output.body, "createBucket");
    }),
    deleteEmpty: Effect.fnUntraced(function* (input: {
      readonly bucket: string;
      readonly ifMetagenerationMatch: string;
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          bucket: Bucket,
          ifMetagenerationMatch: Schema.String.check(Schema.isPattern(/^\d+$/u)),
        }),
        input,
        "deleteBucket",
        "gcs",
        true,
      );
      yield* jsonRequest(storage, "deleteBucket", true, {
        uri: path(value.bucket),
        method: "DELETE",
        qs: { ifMetagenerationMatch: value.ifMetagenerationMatch },
        json: true,
      });
    }),
    configuration: {
      get,
      plan: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly desired: GcsBucketConfiguration;
      }) {
        const desired = yield* decodeRequest(
          Configuration,
          input.desired,
          "planConfiguration",
          "gcs",
        );
        const current = yield* get({ bucket: input.bucket });
        const plan = {
          bucket: current.name,
          expectedMetageneration: current.metageneration,
          desired: structuredClone(desired),
          changes: Object.keys(desired).filter(
            (key) =>
              configurationState(current[key as keyof typeof current]) !==
              configurationState(desired[key as keyof typeof desired]),
          ),
        };
        plans.set(plan, {
          snapshot: configurationState(plan),
          bucket: current.name,
          metageneration: current.metageneration,
          desired: structuredClone(desired),
        });
        return plan;
      }),
      apply: Effect.fnUntraced(function* (input: {
        readonly plan: object;
        readonly protectionChange?: {
          readonly intent: "change-storage-protection";
          readonly reason: string;
        };
      }) {
        const saved = plans.get(input.plan);
        if (!saved || configurationState(input.plan) !== saved.snapshot)
          return yield* storageError(
            "applyConfiguration",
            "gcs",
            { _tag: "InvalidInput", field: "issued-plan" },
            "not-started",
          );
        if (
          [
            "retentionPolicy",
            "softDeletePolicy",
            "iamConfiguration",
            "encryption",
            "lifecycle",
          ].some((key) => Object.hasOwn(saved.desired, key)) &&
          (input.protectionChange?.intent !== "change-storage-protection" ||
            !input.protectionChange.reason)
        )
          return yield* storageError(
            "applyConfiguration",
            "gcs",
            { _tag: "InvalidInput", field: "protectionChange" },
            "not-started",
          );
        plans.delete(input.plan);
        const output = yield* jsonRequest(storage, "applyConfiguration", true, {
          uri: path(saved.bucket),
          method: "PATCH",
          qs: { ifMetagenerationMatch: saved.metageneration },
          json: saved.desired,
        });
        return yield* decodeJson(Native, output.body, "applyConfiguration");
      }),
    },
    lockRetentionPolicy: Effect.fnUntraced(function* (input: {
      readonly bucket: string;
      readonly ifMetagenerationMatch: string;
      readonly intent: "irreversibly-lock-retention";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          bucket: Bucket,
          ifMetagenerationMatch: Schema.String.check(Schema.isPattern(/^\d+$/u)),
          intent: Schema.Literal("irreversibly-lock-retention"),
        }),
        input,
        "lockRetentionPolicy",
        "gcs",
        true,
      );
      const output = yield* jsonRequest(storage, "lockRetentionPolicy", true, {
        uri: `${path(value.bucket)}/lockRetentionPolicy`,
        method: "POST",
        qs: { ifMetagenerationMatch: value.ifMetagenerationMatch },
        json: {},
      });
      return yield* decodeJson(Native, output.body, "lockRetentionPolicy");
    }),
    notifications: {
      list: Effect.fnUntraced(function* (input: { readonly bucket: string }) {
        const value = yield* decodeRequest(
          Schema.Struct({ bucket: Bucket }),
          input,
          "listNotifications",
          "gcs",
        );
        return (yield* jsonRequest(storage, "listNotifications", false, {
          uri: `${path(value.bucket)}/notificationConfigs`,
          json: true,
        })).body;
      }),
      create: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly topic: string;
        readonly events?: ReadonlyArray<string>;
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({
            bucket: Bucket,
            topic: Schema.NonEmptyString,
            events: Schema.optional(Schema.Array(Schema.NonEmptyString)),
          }),
          input,
          "createNotification",
          "gcs",
          true,
        );
        return (yield* jsonRequest(storage, "createNotification", true, {
          uri: `${path(value.bucket)}/notificationConfigs`,
          method: "POST",
          json: { topic: value.topic, payload_format: "JSON_API_V1", event_types: value.events },
        })).body;
      }),
      delete: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly id: string;
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({ bucket: Bucket, id: Schema.NonEmptyString }),
          input,
          "deleteNotification",
          "gcs",
          true,
        );
        yield* jsonRequest(storage, "deleteNotification", true, {
          uri: `${path(value.bucket)}/notificationConfigs/${encodeURIComponent(value.id)}`,
          method: "DELETE",
          json: true,
        });
      }),
    },
  };
};
const make = Effect.fnUntraced(function* (config: Omit<GcsConfig, "bucket">) {
  return makeService(yield* GcsStorage.make({ ...config, bucket: "administration" }));
});
export class GcsAdministration extends Context.Service<
  GcsAdministration,
  ReturnType<typeof makeService>
>()("@effect-kit/storage/gcs/GcsAdministration") {
  static readonly make = make;
  static layer(config: Omit<GcsConfig, "bucket">) {
    return Layer.effect(GcsAdministration, make(config));
  }
}
