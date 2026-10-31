import { Effect, Predicate, Schema } from "effect";

import * as S3 from "@aws-sdk/client-s3";

import { storageError } from "../../core/errors.js";
import type { S3StorageService } from "./index.js";
import { request } from "./transport.js";

export interface S3Configuration {
  readonly cors?: S3.CORSConfiguration;
  readonly versioning?: S3.VersioningConfiguration;
  readonly lifecycle?: S3.BucketLifecycleConfiguration;
  readonly encryption?: S3.ServerSideEncryptionConfiguration;
  readonly publicAccess?: S3.PublicAccessBlockConfiguration;
  readonly policy?: string;
  readonly notifications?: S3.NotificationConfiguration;
  readonly objectLock?: S3.ObjectLockConfiguration;
  readonly replication?: S3.ReplicationConfiguration;
}
export const configurationSections = [
  "cors",
  "versioning",
  "lifecycle",
  "encryption",
  "publicAccess",
  "policy",
  "notifications",
  "objectLock",
  "replication",
] as const;
export type ConfigurationSection = (typeof configurationSections)[number];
export const ConfigurationSection = Schema.Literals(configurationSections);
const NativeObject = Schema.Record(Schema.String, Schema.Unknown);
const shapes = {
  cors: Schema.Struct({
    CORSRules: Schema.Array(
      Schema.Struct({
        AllowedMethods: Schema.Array(Schema.NonEmptyString),
        AllowedOrigins: Schema.Array(Schema.NonEmptyString),
      }),
    ),
  }),
  versioning: Schema.Struct({ Status: Schema.Literals(["Enabled", "Suspended"]) }),
  lifecycle: Schema.Struct({
    Rules: Schema.Array(Schema.Struct({ Status: Schema.Literals(["Enabled", "Disabled"]) })),
  }),
  encryption: Schema.Struct({
    Rules: Schema.Array(
      Schema.Struct({
        ApplyServerSideEncryptionByDefault: Schema.Struct({
          SSEAlgorithm: Schema.Literals(["AES256", "aws:kms", "aws:kms:dsse"]),
        }),
      }),
    ),
  }),
  publicAccess: Schema.Struct({
    BlockPublicAcls: Schema.Boolean,
    IgnorePublicAcls: Schema.Boolean,
    BlockPublicPolicy: Schema.Boolean,
    RestrictPublicBuckets: Schema.Boolean,
  }),
  policy: Schema.String,
  notifications: NativeObject,
  objectLock: Schema.Struct({ ObjectLockEnabled: Schema.Literal("Enabled") }),
  replication: Schema.Struct({ Role: Schema.NonEmptyString, Rules: Schema.Array(NativeObject) }),
};
export const validateConfiguration = Effect.fnUntraced(function* (input: S3Configuration) {
  const object = yield* Schema.decodeUnknownEffect(NativeObject)(input).pipe(
    Effect.mapError(() =>
      storageError("planConfiguration", "s3", { _tag: "InvalidInput", field: "desired" }),
    ),
  );
  for (const [section, value] of Object.entries(object)) {
    if (!configurationSections.some((name) => name === section) || value === undefined)
      return yield* storageError("planConfiguration", "s3", {
        _tag: "InvalidInput",
        field: "section",
      });
    yield* Schema.decodeUnknownEffect(shapes[section as ConfigurationSection])(value).pipe(
      Effect.mapError(() =>
        storageError("planConfiguration", "s3", { _tag: "InvalidInput", field: section }),
      ),
    );
    if (section === "policy") {
      const policy = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NativeObject))(
        value,
      ).pipe(
        Effect.mapError(() =>
          storageError("planConfiguration", "s3", { _tag: "InvalidInput", field: "policy" }),
        ),
      );
      if (!Predicate.isObject(policy))
        return yield* storageError("planConfiguration", "s3", {
          _tag: "InvalidInput",
          field: "policy",
        });
    }
  }
  // Preserve complete SDK-native rules, including fields not projected by validation.
  return input;
});

export const getConfiguration = Effect.fnUntraced(
  function* (
    storage: S3StorageService,
    bucket: string,
    section: ConfigurationSection,
  ): Effect.fn.Return<unknown, ReturnType<typeof storageError>> {
    const Bucket = bucket;
    switch (section) {
      case "cors":
        return (yield* request(storage, "getCors", false, (signal) =>
          storage.client.send(new S3.GetBucketCorsCommand({ Bucket }), { abortSignal: signal }),
        )).CORSRules;
      case "versioning": {
        const { Status, MFADelete } = yield* request(storage, "getVersioning", false, (signal) =>
          storage.client.send(new S3.GetBucketVersioningCommand({ Bucket }), {
            abortSignal: signal,
          }),
        );
        return { Status, MFADelete };
      }
      case "lifecycle":
        return (yield* request(storage, "getLifecycle", false, (signal) =>
          storage.client.send(new S3.GetBucketLifecycleConfigurationCommand({ Bucket }), {
            abortSignal: signal,
          }),
        )).Rules;
      case "encryption":
        return (yield* request(storage, "getEncryption", false, (signal) =>
          storage.client.send(new S3.GetBucketEncryptionCommand({ Bucket }), {
            abortSignal: signal,
          }),
        )).ServerSideEncryptionConfiguration;
      case "publicAccess":
        return (yield* request(storage, "getPublicAccess", false, (signal) =>
          storage.client.send(new S3.GetPublicAccessBlockCommand({ Bucket }), {
            abortSignal: signal,
          }),
        )).PublicAccessBlockConfiguration;
      case "policy":
        return (yield* request(storage, "getPolicy", false, (signal) =>
          storage.client.send(new S3.GetBucketPolicyCommand({ Bucket }), { abortSignal: signal }),
        )).Policy;
      case "notifications": {
        const {
          TopicConfigurations,
          QueueConfigurations,
          LambdaFunctionConfigurations,
          EventBridgeConfiguration,
        } = yield* request(storage, "getNotifications", false, (signal) =>
          storage.client.send(new S3.GetBucketNotificationConfigurationCommand({ Bucket }), {
            abortSignal: signal,
          }),
        );
        return {
          TopicConfigurations,
          QueueConfigurations,
          LambdaFunctionConfigurations,
          EventBridgeConfiguration,
        };
      }
      case "objectLock":
        return (yield* request(storage, "getObjectLock", false, (signal) =>
          storage.client.send(new S3.GetObjectLockConfigurationCommand({ Bucket }), {
            abortSignal: signal,
          }),
        )).ObjectLockConfiguration;
      case "replication":
        return (yield* request(storage, "getReplication", false, (signal) =>
          storage.client.send(new S3.GetBucketReplicationCommand({ Bucket }), {
            abortSignal: signal,
          }),
        )).ReplicationConfiguration;
    }
  },
  Effect.catch((error) =>
    error.reason._tag === "ProviderFailure" &&
    [
      "NoSuchCORSConfiguration",
      "NoSuchLifecycleConfiguration",
      "ServerSideEncryptionConfigurationNotFoundError",
      "NoSuchPublicAccessBlockConfiguration",
      "NoSuchBucketPolicy",
      "ObjectLockConfigurationNotFoundError",
      "ReplicationConfigurationNotFoundError",
    ].includes(error.reason.code ?? "")
      ? Effect.succeed(undefined)
      : Effect.fail(error),
  ),
);
export function desiredSection(
  configuration: S3Configuration,
  section: ConfigurationSection,
): unknown {
  const value = configuration[section];
  return section === "cors"
    ? configuration.cors?.CORSRules
    : section === "lifecycle"
      ? configuration.lifecycle?.Rules
      : value;
}
export const putConfiguration = Effect.fnUntraced(function* (
  storage: S3StorageService,
  Bucket: string,
  section: ConfigurationSection,
  value: S3Configuration,
) {
  switch (section) {
    case "cors":
      yield* request(storage, "putCors", true, (signal) =>
        storage.client.send(
          new S3.PutBucketCorsCommand({ Bucket, CORSConfiguration: value.cors }),
          { abortSignal: signal },
        ),
      );
      break;
    case "versioning":
      yield* request(storage, "putVersioning", true, (signal) =>
        storage.client.send(
          new S3.PutBucketVersioningCommand({ Bucket, VersioningConfiguration: value.versioning }),
          { abortSignal: signal },
        ),
      );
      break;
    case "lifecycle":
      yield* request(storage, "putLifecycle", true, (signal) =>
        storage.client.send(
          new S3.PutBucketLifecycleConfigurationCommand({
            Bucket,
            LifecycleConfiguration: value.lifecycle,
          }),
          { abortSignal: signal },
        ),
      );
      break;
    case "encryption":
      yield* request(storage, "putEncryption", true, (signal) =>
        storage.client.send(
          new S3.PutBucketEncryptionCommand({
            Bucket,
            ServerSideEncryptionConfiguration: value.encryption,
          }),
          { abortSignal: signal },
        ),
      );
      break;
    case "publicAccess":
      yield* request(storage, "putPublicAccess", true, (signal) =>
        storage.client.send(
          new S3.PutPublicAccessBlockCommand({
            Bucket,
            PublicAccessBlockConfiguration: value.publicAccess,
          }),
          { abortSignal: signal },
        ),
      );
      break;
    case "policy":
      yield* request(storage, "putPolicy", true, (signal) =>
        storage.client.send(new S3.PutBucketPolicyCommand({ Bucket, Policy: value.policy }), {
          abortSignal: signal,
        }),
      );
      break;
    case "notifications":
      yield* request(storage, "putNotifications", true, (signal) =>
        storage.client.send(
          new S3.PutBucketNotificationConfigurationCommand({
            Bucket,
            NotificationConfiguration: value.notifications,
          }),
          { abortSignal: signal },
        ),
      );
      break;
    case "objectLock":
      yield* request(storage, "putObjectLock", true, (signal) =>
        storage.client.send(
          new S3.PutObjectLockConfigurationCommand({
            Bucket,
            ObjectLockConfiguration: value.objectLock,
          }),
          { abortSignal: signal },
        ),
      );
      break;
    case "replication":
      yield* request(storage, "putReplication", true, (signal) =>
        storage.client.send(
          new S3.PutBucketReplicationCommand({
            Bucket,
            ReplicationConfiguration: value.replication,
          }),
          { abortSignal: signal },
        ),
      );
      break;
  }
});
