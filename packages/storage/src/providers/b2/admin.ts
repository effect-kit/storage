import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";

import { storageError } from "../../core/errors.js";
import { controlEndpoint, controlRequest } from "../../internal/http/control-plane.js";
import { decodeResponse } from "../../internal/provider-response.js";
import { decodeRequest } from "../../internal/request-validation.js";

export interface B2AdminConfig {
  readonly applicationKeyId: string;
  readonly applicationKey: Redacted.Redacted<string>;
  readonly accountEndpoint?: string;
}
const Native = Schema.Record(Schema.String, Schema.Unknown);
const File = Schema.Struct({ fileId: Schema.NonEmptyString, fileName: Schema.NonEmptyString });
const make = Effect.fnUntraced(function* (settings: B2AdminConfig) {
  const config = yield* decodeRequest(
    Schema.Struct({
      applicationKeyId: Schema.NonEmptyString,
      applicationKey: Schema.Redacted(Schema.NonEmptyString),
      accountEndpoint: Schema.optional(Schema.String),
    }),
    settings,
    "makeAdministration",
    "b2",
  );
  const endpoint = yield* controlEndpoint(
    config.accountEndpoint ?? "https://api.backblazeb2.com",
    "b2",
  );
  const http = yield* HttpClient.HttpClient;
  const call = Effect.fnUntraced(function* (
    operation: string,
    method: string,
    body: Readonly<Record<string, unknown>>,
  ) {
    const auth = yield* controlRequest(
      http,
      "b2",
      "authorizeAccount",
      `${endpoint}/b2api/v4/b2_authorize_account`,
      "GET",
      {
        authorization: `Basic ${Buffer.from(`${config.applicationKeyId}:${Redacted.value(config.applicationKey)}`).toString("base64")}`,
      },
    );
    const identity = yield* decodeResponse(
      Schema.Struct({
        accountId: Schema.NonEmptyString,
        authorizationToken: Schema.NonEmptyString,
        apiInfo: Schema.Struct({ storageApi: Schema.Struct({ apiUrl: Schema.String }) }),
      }),
      auth,
      "authorizeAccount",
      "b2",
    );
    const api = yield* controlEndpoint(identity.apiInfo.storageApi.apiUrl, "b2");
    if (api !== endpoint && !new URL(api).hostname.endsWith(".backblazeb2.com"))
      return yield* storageError(
        operation,
        "b2",
        { _tag: "ProviderFailure", code: "UntrustedApiOrigin" },
        "not-started",
      );
    const result = yield* controlRequest(
      http,
      "b2",
      operation,
      `${api}/b2api/v4/${method}`,
      "POST",
      { authorization: identity.authorizationToken },
      { accountId: identity.accountId, ...body },
    );
    return yield* decodeRequest(Native, result, operation, "b2");
  });
  return {
    listBuckets: () => call("listBuckets", "b2_list_buckets", {}),
    create: Effect.fnUntraced(function* (input: { readonly name: string }) {
      const value = yield* decodeRequest(
        Schema.Struct({ name: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9-]{6,63}$/u)) }),
        input,
        "createBucket",
        "b2",
        true,
      );
      return yield* call("createBucket", "b2_create_bucket", {
        bucketName: value.name,
        bucketType: "allPrivate",
      });
    }),
    deleteEmpty: Effect.fnUntraced(function* (input: { readonly bucketId: string }) {
      const value = yield* decodeRequest(
        Schema.Struct({ bucketId: Schema.NonEmptyString }),
        input,
        "deleteBucket",
        "b2",
        true,
      );
      return yield* call("deleteBucket", "b2_delete_bucket", value);
    }),
    updateBucket: Effect.fnUntraced(function* (input: {
      readonly bucketId: string;
      readonly expectedRevision: number;
      readonly configuration: Readonly<Record<string, unknown>>;
      readonly intent: "change-storage-protection";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          bucketId: Schema.NonEmptyString,
          expectedRevision: Schema.Natural,
          configuration: Schema.Struct({
            bucketType: Schema.optional(Schema.Literals(["allPublic", "allPrivate"])),
            lifecycleRules: Schema.optional(Schema.Array(Native)),
            defaultServerSideEncryption: Schema.optional(Native),
            defaultRetention: Schema.optional(Native),
            replicationConfiguration: Schema.optional(Native),
            corsRules: Schema.optional(Schema.Array(Native)),
          }),
          intent: Schema.Literal("change-storage-protection"),
        }),
        input,
        "updateBucket",
        "b2",
        true,
      );
      return yield* call("updateBucket", "b2_update_bucket", {
        bucketId: value.bucketId,
        ifRevisionIs: value.expectedRevision,
        ...value.configuration,
      });
    }),
    listFileVersions: Effect.fnUntraced(function* (input: {
      readonly bucketId: string;
      readonly prefix?: string;
      readonly startFileName?: string;
      readonly startFileId?: string;
      readonly limit?: number;
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          bucketId: Schema.NonEmptyString,
          prefix: Schema.optional(Schema.String),
          startFileName: Schema.optional(Schema.String),
          startFileId: Schema.optional(Schema.String),
          limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
        }),
        input,
        "listFileVersions",
        "b2",
      );
      return yield* call("listFileVersions", "b2_list_file_versions", {
        bucketId: value.bucketId,
        prefix: value.prefix,
        startFileName: value.startFileName,
        startFileId: value.startFileId,
        maxFileCount: value.limit ?? 1000,
      });
    }),
    hideFile: Effect.fnUntraced(function* (input: {
      readonly bucketId: string;
      readonly fileName: string;
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({ bucketId: Schema.NonEmptyString, fileName: Schema.NonEmptyString }),
        input,
        "hideFile",
        "b2",
        true,
      );
      return yield* call("hideFile", "b2_hide_file", value);
    }),
    deleteFileVersion: Effect.fnUntraced(function* (
      input: typeof File.Type & {
        readonly intent: "delete-exact-file-version";
        readonly bypassGovernance?: {
          readonly intent: "bypass-governance";
          readonly reason: string;
        };
      },
    ) {
      const value = yield* decodeRequest(
        Schema.Struct({
          ...File.fields,
          intent: Schema.Literal("delete-exact-file-version"),
          bypassGovernance: Schema.optional(
            Schema.Struct({
              intent: Schema.Literal("bypass-governance"),
              reason: Schema.NonEmptyString,
            }),
          ),
        }),
        input,
        "deleteFileVersion",
        "b2",
        true,
      );
      return yield* call("deleteFileVersion", "b2_delete_file_version", {
        fileId: value.fileId,
        fileName: value.fileName,
        ...(value.bypassGovernance ? { bypassGovernance: true } : {}),
      });
    }),
    restoreVersion: Effect.fnUntraced(function* (input: {
      readonly sourceFileId: string;
      readonly destinationBucketId: string;
      readonly fileName: string;
      readonly intent: "create-new-file-version";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          sourceFileId: Schema.NonEmptyString,
          destinationBucketId: Schema.NonEmptyString,
          fileName: Schema.NonEmptyString,
          intent: Schema.Literal("create-new-file-version"),
        }),
        input,
        "restoreVersion",
        "b2",
        true,
      );
      return yield* call("restoreVersion", "b2_copy_file", {
        sourceFileId: value.sourceFileId,
        destinationBucketId: value.destinationBucketId,
        fileName: value.fileName,
        metadataDirective: "COPY",
      });
    }),
    setLegalHold: Effect.fnUntraced(function* (
      input: typeof File.Type & {
        readonly status: "on" | "off";
        readonly releaseIntent?: "release-object-hold";
      },
    ) {
      const value = yield* decodeRequest(
        Schema.Struct({
          ...File.fields,
          status: Schema.Literals(["on", "off"]),
          releaseIntent: Schema.optional(Schema.Literal("release-object-hold")),
        }),
        input,
        "setLegalHold",
        "b2",
        true,
      );
      if (value.status === "off" && !value.releaseIntent)
        return yield* storageError(
          "setLegalHold",
          "b2",
          { _tag: "InvalidInput", field: "releaseIntent" },
          "not-started",
        );
      return yield* call("setLegalHold", "b2_update_file_legal_hold", {
        fileName: value.fileName,
        fileId: value.fileId,
        legalHold: value.status,
      });
    }),
  };
});
export class B2Administration extends Context.Service<
  B2Administration,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/b2/B2Administration") {
  static readonly make = make;
  static layerHttp(config: B2AdminConfig) {
    return Layer.effect(B2Administration, make(config));
  }
  static layer(config: B2AdminConfig) {
    return B2Administration.layerHttp(config).pipe(Layer.provide(FetchHttpClient.layer));
  }
}
