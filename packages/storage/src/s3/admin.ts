import { Context, Effect, Layer, Result, Schema } from "effect";

import {
  CreateBucketCommand,
  DeleteBucketCommand,
  HeadBucketCommand,
  ListBucketsCommand,
} from "@aws-sdk/client-s3";
import type { CreateBucketConfiguration } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { configurationState } from "../lifecycle/configuration-state.js";
import { S3Storage } from "../s3.js";
import type { S3Config, S3StorageService } from "../s3.js";
import {
  ConfigurationSection,
  configurationSections,
  desiredSection,
  getConfiguration,
  putConfiguration,
  validateConfiguration,
} from "./admin-configuration.js";
import type { S3Configuration } from "./admin-configuration.js";
import { request } from "./transport.js";

export type { S3Configuration, ConfigurationSection } from "./admin-configuration.js";
export interface S3AdminConfig extends Omit<S3Config, "bucket" | "storeId"> {
  readonly credentials: NonNullable<S3Config["credentials"]>;
}
const Bucket = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u));
const Target = Schema.Struct({ bucket: Bucket });
export interface ConfigurationPlan {
  readonly bucket: string;
  readonly changes: ReadonlyArray<{
    readonly section: ConfigurationSection;
    readonly before: unknown;
    readonly after: unknown;
  }>;
  readonly concurrency: "best-effort";
}
export interface ConfigurationOutcome {
  readonly bucket: string;
  readonly steps: ReadonlyArray<{
    readonly section: ConfigurationSection;
    readonly status: "applied" | "pending" | "failed" | "unknown";
    readonly error?: StorageError | undefined;
  }>;
  readonly complete: boolean;
}

export function makeAdministration(storage: S3StorageService) {
  const issued = new WeakMap<
    ConfigurationPlan,
    {
      bucket: string;
      desired: S3Configuration;
      snapshot: string;
      before: ReadonlyArray<{ section: ConfigurationSection; value: unknown }>;
    }
  >();
  const consumed = new WeakSet<ConfigurationPlan>();
  const allowedSections =
    storage.provider === "s3" || storage.provider === "minio"
      ? configurationSections
      : storage.provider === "wasabi"
        ? ["cors", "versioning", "lifecycle", "objectLock", "policy", "replication"]
        : storage.provider === "spaces"
          ? storage.profile?.bucketTier === "cold"
            ? ["lifecycle"]
            : ["cors", "versioning", "lifecycle", "policy"]
          : ["cors", "versioning", "lifecycle", "encryption", "objectLock", "policy"];
  const read = Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly sections: ReadonlyArray<ConfigurationSection>;
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({ bucket: Bucket, sections: Schema.Array(ConfigurationSection) }),
      input,
      "getConfiguration",
      storage.provider,
    );
    if (value.sections.some((section) => !allowedSections.includes(section)))
      return yield* storageError("getConfiguration", storage.provider, {
        _tag: "Unsupported",
        capability: "configuration-section",
      });
    return yield* Effect.forEach(value.sections, (section) =>
      getConfiguration(storage, value.bucket, section).pipe(
        Effect.map((configuration) => ({ section, value: configuration })),
      ),
    );
  });
  return {
    listPage: Effect.fnUntraced(function* (
      input: { readonly cursor?: string; readonly limit?: number } = {},
    ) {
      const value = yield* decodeRequest(
        Schema.Struct({
          cursor: Schema.optional(Schema.NonEmptyString),
          limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
        }),
        input,
        "listBuckets",
        storage.provider,
      );
      const page = yield* request(storage, "listBuckets", false, (signal) =>
        storage.client.send(
          new ListBucketsCommand({
            ContinuationToken: value.cursor,
            MaxBuckets: value.limit ?? 1000,
          }),
          { abortSignal: signal },
        ),
      );
      return {
        buckets: (page.Buckets ?? []).map(({ Name, CreationDate }) => ({
          name: Name,
          createdAt: CreationDate,
        })),
        cursor: page.ContinuationToken,
      };
    }),
    head: Effect.fnUntraced(function* (input: typeof Target.Type) {
      const value = yield* decodeRequest(Target, input, "headBucket", storage.provider);
      const result = yield* request(storage, "headBucket", false, (signal) =>
        storage.client.send(new HeadBucketCommand({ Bucket: value.bucket }), {
          abortSignal: signal,
        }),
      );
      return { bucket: value.bucket, region: result.BucketRegion };
    }),
    create: Effect.fnUntraced(function* (input: {
      readonly bucket: string;
      readonly configuration?: CreateBucketConfiguration;
      readonly objectLockEnabled?: boolean;
    }) {
      yield* decodeRequest(
        Target,
        { bucket: input.bucket },
        "createBucket",
        storage.provider,
        true,
      );
      const region = input.configuration?.LocationConstraint;
      if (
        (region && region !== storage.config.region) ||
        (!storage.profile && storage.config.region !== "us-east-1" && !region)
      )
        return yield* storageError(
          "createBucket",
          storage.provider,
          { _tag: "InvalidInput", field: "location" },
          "not-started",
        );
      yield* request(storage, "createBucket", true, (signal) =>
        storage.client.send(
          new CreateBucketCommand({
            Bucket: input.bucket,
            ...(input.configuration ? { CreateBucketConfiguration: input.configuration } : {}),
            ...(input.objectLockEnabled ? { ObjectLockEnabledForBucket: true } : {}),
          }),
          { abortSignal: signal },
        ),
      );
      return { _tag: "Accepted" as const, bucket: input.bucket };
    }),
    // The provider's atomic nonempty rejection is authoritative; never enumerate then assume empty.
    deleteEmpty: Effect.fnUntraced(function* (input: typeof Target.Type) {
      const value = yield* decodeRequest(Target, input, "deleteBucket", storage.provider, true);
      yield* request(storage, "deleteBucket", true, (signal) =>
        storage.client.send(new DeleteBucketCommand({ Bucket: value.bucket }), {
          abortSignal: signal,
        }),
      );
    }),
    configuration: {
      get: read,
      plan: Effect.fnUntraced(function* (input: {
        readonly bucket: string;
        readonly desired: S3Configuration;
      }) {
        yield* decodeRequest(
          Target,
          { bucket: input.bucket },
          "planConfiguration",
          storage.provider,
        );
        const desired = structuredClone(yield* validateConfiguration(input.desired));
        const sections = configurationSections.filter((section) => desired[section] !== undefined);
        const before = yield* read({ bucket: input.bucket, sections });
        const changes = before
          .filter(
            ({ section, value }) =>
              configurationState(value) !== configurationState(desiredSection(desired, section)),
          )
          .map(({ section, value }) => ({
            section,
            before: structuredClone(value),
            after: structuredClone(desiredSection(desired, section)),
          }));
        const plan: ConfigurationPlan = Object.freeze({
          bucket: input.bucket,
          changes: Object.freeze(changes),
          concurrency: "best-effort",
        });
        issued.set(plan, {
          bucket: input.bucket,
          desired,
          before,
          snapshot: configurationState(plan),
        });
        return plan;
      }),
      apply: Effect.fnUntraced(function* (input: {
        readonly plan: ConfigurationPlan;
        readonly allowUnconditional: true;
        readonly protectionChange?: {
          readonly intent: "change-storage-protection";
          readonly reason: string;
        };
      }): Effect.fn.Return<ConfigurationOutcome, StorageError> {
        const saved = issued.get(input.plan);
        if (
          !saved ||
          configurationState(input.plan) !== saved.snapshot ||
          consumed.has(input.plan) ||
          input.allowUnconditional !== true
        )
          return yield* storageError(
            "applyConfiguration",
            storage.provider,
            { _tag: "InvalidInput", field: "issued-plan-and-concurrency-intent" },
            "not-started",
          );
        if (
          Object.keys(saved.desired).some((section) =>
            [
              "publicAccess",
              "policy",
              "objectLock",
              "versioning",
              "encryption",
              "lifecycle",
            ].includes(section),
          ) &&
          (input.protectionChange?.intent !== "change-storage-protection" ||
            !input.protectionChange.reason)
        )
          return yield* storageError(
            "applyConfiguration",
            storage.provider,
            { _tag: "InvalidInput", field: "protectionChange" },
            "not-started",
          );
        const current = yield* read({
          bucket: saved.bucket,
          sections: saved.before.map((entry) => entry.section),
        });
        if (configurationState(current) !== configurationState(saved.before))
          return yield* storageError(
            "applyConfiguration",
            storage.provider,
            { _tag: "PreconditionFailed" },
            "not-started",
          );
        consumed.add(input.plan);
        const steps: ConfigurationOutcome["steps"][number][] = [];
        for (const { section, value } of saved.before) {
          if (
            configurationState(value) === configurationState(desiredSection(saved.desired, section))
          )
            continue;
          const outcome = yield* Effect.result(
            putConfiguration(storage, saved.bucket, section, saved.desired),
          );
          if (Result.isFailure(outcome)) {
            steps.push({
              section,
              status: outcome.failure.mutationOutcome === "unknown" ? "unknown" : "failed",
              error: outcome.failure,
            });
            break;
          }
          const observed = yield* Effect.result(getConfiguration(storage, saved.bucket, section));
          steps.push({
            section,
            status:
              Result.isSuccess(observed) &&
              configurationState(observed.success) ===
                configurationState(desiredSection(saved.desired, section))
                ? "applied"
                : "pending",
            ...(Result.isFailure(observed) ? { error: observed.failure } : {}),
          });
        }
        return {
          bucket: saved.bucket,
          steps,
          complete:
            steps.every((step) => step.status === "applied") &&
            steps.length === input.plan.changes.length,
        };
      }),
    },
  };
}
const make = Effect.fnUntraced(function* (config: S3AdminConfig) {
  if (!config.credentials)
    return yield* storageError("makeAdministration", "s3", {
      _tag: "InvalidConfiguration",
      field: "credentials",
    });
  return makeAdministration(yield* S3Storage.make({ ...config, bucket: "administration" }));
});
export type S3AdministrationService = ReturnType<typeof makeAdministration>;
export class S3Administration extends Context.Service<S3Administration, S3AdministrationService>()(
  "@effect-kit/storage/s3/S3Administration",
) {
  static readonly make = make;
  static layer(config: S3AdminConfig) {
    return Layer.effect(S3Administration, make(config));
  }
}
