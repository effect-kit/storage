import { Context, Effect, Layer, Redacted, Schema } from "effect";

import { DeleteBucketCommand, DeleteObjectCommand, RestoreObjectCommand } from "@aws-sdk/client-s3";
import * as Tigris from "@tigrisdata/storage";

import { storageError } from "../../core/errors.js";
import { controlEndpoint } from "../../internal/http/control-plane.js";
import { providerError } from "../../internal/node/provider-errors.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { VersionRef } from "../../lifecycle/index.js";
import { S3Archive } from "../s3/archive.js";
import { S3Storage } from "../s3/index.js";
import type { S3StorageService } from "../s3/index.js";
import { lifecycleTarget } from "../s3/lifecycle-validation.js";
import { extendRequest } from "../s3/request-extension.js";
import { request } from "../s3/transport.js";
import { configuration } from "./configuration.js";
import { TigrisStorage } from "./index.js";
import { Bucket, createBucket, snapshots } from "./snapshots.js";
import { call } from "./transport.js";

export interface TigrisNativeConfig {
  readonly endpoint: string;
  readonly organizationId?: string;
  readonly credentials: NonNullable<Tigris.TigrisConfig["credentialProvider"]>;
}
const Name = Bucket;
const bucket = (name: string) => decodeRequest(Name, name, "bucket", "tigris");

const make = Effect.fnUntraced(function* (settings: TigrisNativeConfig) {
  const config = yield* decodeRequest(
    Schema.Struct({
      endpoint: Schema.String,
      organizationId: Schema.optional(Schema.NonEmptyString),
      credentials: Schema.declare<TigrisNativeConfig["credentials"]>(
        (value): value is TigrisNativeConfig["credentials"] => typeof value === "function",
      ),
    }),
    settings,
    "makeAdministration",
    "tigris",
  );
  yield* controlEndpoint(config.endpoint, "tigris");
  const storage = yield* TigrisStorage.make({
    bucket: "administration",
    endpoint: config.endpoint,
    credentials: config.credentials,
    forcePathStyle: true,
    placement: "global",
  });
  const nativeConfiguration = Effect.tryPromise({
    try: config.credentials,
    catch: () => providerError("tigris", "credentials", false, {}),
  }).pipe(
    Effect.flatMap((credentials) => {
      if (
        !credentials.accessKeyId ||
        !credentials.secretAccessKey ||
        (credentials.sessionToken && !config.organizationId)
      )
        return Effect.fail(
          storageError("makeAdministration", "tigris", {
            _tag: "InvalidConfiguration",
            field: "credentials-or-organizationId",
          }),
        );
      return Effect.succeed({
        endpoint: config.endpoint,
        mgmtEndpoint: config.endpoint,
        iamEndpoint: config.endpoint,
        accessKeyId: credentials.accessKeyId,
        secretAccessKey: credentials.secretAccessKey,
        sessionToken: credentials.sessionToken ?? "",
        organizationId: config.organizationId ?? "",
        retry: false,
      } satisfies Tigris.TigrisConfig);
    }),
  );
  const invoke = <A>(
    operation: string,
    mutation: boolean,
    execute: (
      native: Tigris.TigrisConfig,
    ) => Promise<{ data: A; error?: never } | { error: Error; data?: never }>,
  ) =>
    nativeConfiguration.pipe(
      Effect.flatMap((current) => call(operation, mutation, () => execute(current))),
    );
  const pagination = Schema.Struct({
    cursor: Schema.optional(Schema.NonEmptyString),
    limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
  });
  return {
    configuration: configuration(nativeConfiguration),
    listPage: (
      input: { readonly cursor?: string; readonly limit?: number; readonly deleted?: boolean } = {},
    ) =>
      decodeRequest(
        Schema.Struct({ ...pagination.fields, deleted: Schema.optional(Schema.Boolean) }),
        input,
        "listBuckets",
        "tigris",
      ).pipe(
        Effect.flatMap((value) =>
          invoke("listBuckets", false, (native) =>
            Tigris.listBuckets({
              config: native,
              ...(value.cursor ? { paginationToken: value.cursor } : {}),
              ...(value.limit ? { limit: value.limit } : {}),
              ...(value.deleted !== undefined ? { deleted: value.deleted } : {}),
            }),
          ),
        ),
      ),
    head: (input: { readonly bucket: string }) =>
      bucket(input.bucket).pipe(
        Effect.flatMap((name) =>
          invoke("headBucket", false, (native) => Tigris.getBucketInfo(name, { config: native })),
        ),
        Effect.map((info) => ({ ...info, settings: Redacted.make(info.settings) })),
      ),
    create: (input: { readonly bucket: string }) => createBucket(storage, input),
    deleteEmpty: Effect.fnUntraced(function* (input: { readonly bucket: string }) {
      const name = yield* bucket(input.bucket);
      yield* request(storage, "deleteBucket", true, (signal) =>
        storage.client.send(new DeleteBucketCommand({ Bucket: name }), { abortSignal: signal }),
      );
    }),
    restoreDeleted: (input: { readonly bucket: string }) =>
      bucket(input.bucket).pipe(
        Effect.flatMap((name) =>
          invoke("restoreBucket", true, (native) => Tigris.restoreBucket(name, { config: native })),
        ),
      ),
    snapshots: {
      ...snapshots(storage),
      enable: (input: { readonly bucket: string }) =>
        bucket(input.bucket).pipe(
          Effect.flatMap((name) =>
            invoke("enableSnapshots", true, (native) =>
              Tigris.enableSnapshot(name, { config: native }),
            ),
          ),
        ),
    },
    forks: {
      listPage: (input: {
        readonly bucket: string;
        readonly cursor?: string;
        readonly limit?: number;
      }) =>
        decodeRequest(
          Schema.Struct({ bucket: Bucket, ...pagination.fields }),
          input,
          "listForks",
          "tigris",
        ).pipe(
          Effect.flatMap((value) =>
            invoke("listForks", false, (native) =>
              Tigris.listForks(value.bucket, {
                config: native,
                ...(value.cursor ? { paginationToken: value.cursor } : {}),
                ...(value.limit ? { limit: value.limit } : {}),
              }),
            ),
          ),
        ),
      create: (input: {
        readonly bucket: string;
        readonly sourceBucket: string;
        readonly snapshot: string;
      }) => createBucket(storage, input),
      merge: Effect.fnUntraced(function* (input: {
        readonly fork: string;
        readonly destination: string;
        readonly intent: "merge-fork";
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({
            fork: Bucket,
            destination: Bucket,
            intent: Schema.Literal("merge-fork"),
          }),
          input,
          "mergeFork",
          "tigris",
          true,
        );
        return yield* invoke("mergeFork", true, (native) =>
          Tigris.mergeFork(value.fork, value.destination, { config: native }),
        );
      }),
      rebase: Effect.fnUntraced(function* (input: {
        readonly fork: string;
        readonly intent: "rebase-fork";
      }) {
        const value = yield* decodeRequest(
          Schema.Struct({ fork: Bucket, intent: Schema.Literal("rebase-fork") }),
          input,
          "rebaseFork",
          "tigris",
          true,
        );
        return yield* invoke("rebaseFork", true, (native) =>
          Tigris.rebaseFork(value.fork, { config: native }),
        );
      }),
    },
  };
});
export class TigrisAdministration extends Context.Service<
  TigrisAdministration,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/tigris/TigrisAdministration") {
  static readonly make = make;
  static layer(config: TigrisNativeConfig) {
    return Layer.effect(TigrisAdministration, make(config));
  }
}
const recovery = (storage: S3StorageService) => ({
  restoreDeleted: Effect.fnUntraced(function* (input: { readonly ref: VersionRef }) {
    const value = yield* decodeRequest(
      Schema.Struct({ ref: VersionRef }),
      input,
      "restoreDeleted",
      "tigris",
      true,
    );
    if (storage.provider !== "tigris")
      return yield* storageError("restoreDeleted", "tigris", {
        _tag: "InvalidConfiguration",
        field: "provider",
      });
    const target = yield* lifecycleTarget(storage, value.ref, "restoreDeleted", true);
    const command = new RestoreObjectCommand({ Bucket: target.Bucket, Key: target.Key });
    extendRequest(command, {
      headers: {
        "X-Tigris-Restore-Type": "soft-delete",
        "X-Tigris-Restore-Version": value.ref.version.id,
      },
    });
    yield* request(storage, "restoreDeleted", true, (signal) =>
      storage.client.send(command, { abortSignal: signal }),
    );
    return { _tag: "Accepted" as const, source: value.ref };
  }),
  purgeDeleted: Effect.fnUntraced(function* (input: {
    readonly ref: VersionRef;
    readonly intent: "purge-deleted-version";
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({ ref: VersionRef, intent: Schema.Literal("purge-deleted-version") }),
      input,
      "purgeDeleted",
      "tigris",
      true,
    );
    if (storage.provider !== "tigris")
      return yield* storageError("purgeDeleted", "tigris", {
        _tag: "InvalidConfiguration",
        field: "provider",
      });
    const target = yield* lifecycleTarget(storage, value.ref, "purgeDeleted", true);
    const command = new DeleteObjectCommand(target);
    extendRequest(command, { headers: { "X-Tigris-Soft-Delete": "true" } });
    yield* request(storage, "purgeDeleted", true, (signal) =>
      storage.client.send(command, { abortSignal: signal }),
    );
  }),
  archive: S3Archive.make(storage),
});
export class TigrisRecovery extends Context.Service<TigrisRecovery, ReturnType<typeof recovery>>()(
  "@effect-kit/storage/tigris/TigrisRecovery",
) {
  static readonly make = recovery;
  static readonly layer = Layer.effect(TigrisRecovery, Effect.map(S3Storage, recovery));
}
