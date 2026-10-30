import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";

import { storageError } from "../errors.js";
import { controlEndpoint, controlRequest } from "../internal/http/control-plane.js";
import { decodeRequest } from "../internal/request-validation.js";

export interface AzureAccountConfig {
  readonly subscriptionId: string;
  readonly resourceGroup: string;
  readonly account: string;
  readonly apiVersion: string;
  readonly endpoint?: string;
  readonly accessToken: Effect.Effect<Redacted.Redacted<string>, ReturnType<typeof storageError>>;
}
const Native = Schema.Record(Schema.String, Schema.Unknown);
const make = Effect.fnUntraced(function* (settings: AzureAccountConfig) {
  const config = yield* decodeRequest(
    Schema.Struct({
      subscriptionId: Schema.NonEmptyString,
      resourceGroup: Schema.NonEmptyString,
      account: Schema.String.check(Schema.isPattern(/^[a-z0-9]{3,24}$/u)),
      apiVersion: Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}(?:-preview)?$/u)),
      endpoint: Schema.optional(Schema.String),
      accessToken: Schema.declare<AzureAccountConfig["accessToken"]>(
        (value): value is AzureAccountConfig["accessToken"] => Effect.isEffect(value),
      ),
    }),
    settings,
    "makeAccountAdministration",
    "azure",
  );
  const endpoint = yield* controlEndpoint(
    config.endpoint ?? "https://management.azure.com",
    "azure",
  );
  const http = yield* HttpClient.HttpClient;
  const root = `${endpoint}/subscriptions/${encodeURIComponent(config.subscriptionId)}/resourceGroups/${encodeURIComponent(config.resourceGroup)}/providers/Microsoft.Storage/storageAccounts/${config.account}`;
  const call = Effect.fnUntraced(function* (
    operation: string,
    method: "GET" | "PUT" | "PATCH",
    suffix: string,
    properties?: unknown,
  ) {
    const token = yield* config.accessToken;
    if (
      !Redacted.isRedacted(token) ||
      !Redacted.value(token) ||
      /[\r\n]/u.test(Redacted.value(token))
    )
      return yield* storageError(operation, "azure", { _tag: "AccessDenied" }, "not-started");
    const output = yield* controlRequest(
      http,
      "azure",
      operation,
      `${root}${suffix}?api-version=${config.apiVersion}`,
      method,
      { authorization: `Bearer ${Redacted.value(token)}` },
      properties === undefined ? undefined : { properties },
    );
    return {
      _tag: method === "GET" ? ("Observed" as const) : ("Accepted" as const),
      resource: output,
    };
  });
  return {
    getAccount: () => call("getAccount", "GET", ""),
    getBlobConfiguration: () => call("getBlobConfiguration", "GET", "/blobServices/default"),
    getLifecycle: () => call("getLifecycle", "GET", "/managementPolicies/default"),
    getReplication: (input: { readonly policyId: string }) =>
      decodeRequest(Schema.NonEmptyString, input.policyId, "getReplication", "azure").pipe(
        Effect.flatMap((id) =>
          call("getReplication", "GET", `/objectReplicationPolicies/${encodeURIComponent(id)}`),
        ),
      ),
    setBlobConfiguration: Effect.fnUntraced(function* (input: {
      readonly properties: Readonly<Record<string, unknown>>;
      readonly intent: "replace-blob-service-configuration";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          properties: Schema.Struct({
            cors: Schema.optional(Native),
            isVersioningEnabled: Schema.optional(Schema.Boolean),
            deleteRetentionPolicy: Schema.optional(Native),
            containerDeleteRetentionPolicy: Schema.optional(Native),
            restorePolicy: Schema.optional(Native),
            changeFeed: Schema.optional(Native),
            defaultServiceVersion: Schema.optional(Schema.String),
          }),
          intent: Schema.Literal("replace-blob-service-configuration"),
        }),
        input,
        "setBlobConfiguration",
        "azure",
        true,
      );
      return yield* call("setBlobConfiguration", "PUT", "/blobServices/default", value.properties);
    }),
    setAccountProtection: Effect.fnUntraced(function* (input: {
      readonly properties: Readonly<Record<string, unknown>>;
      readonly intent: "change-account-protection";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          properties: Schema.Struct({
            allowBlobPublicAccess: Schema.optional(Schema.Boolean),
            minimumTlsVersion: Schema.optional(Schema.Literals(["TLS1_2", "TLS1_3"])),
            encryption: Schema.optional(Native),
            publicNetworkAccess: Schema.optional(Schema.Literals(["Enabled", "Disabled"])),
          }),
          intent: Schema.Literal("change-account-protection"),
        }),
        input,
        "setAccountProtection",
        "azure",
        true,
      );
      return yield* call("setAccountProtection", "PATCH", "", value.properties);
    }),
    setLifecycle: Effect.fnUntraced(function* (input: {
      readonly rules: ReadonlyArray<Readonly<Record<string, unknown>>>;
      readonly intent: "replace-lifecycle-policy";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          rules: Schema.Array(Native),
          intent: Schema.Literal("replace-lifecycle-policy"),
        }),
        input,
        "setLifecycle",
        "azure",
        true,
      );
      return yield* call("setLifecycle", "PUT", "/managementPolicies/default", {
        policy: { rules: value.rules },
      });
    }),
    setReplication: Effect.fnUntraced(function* (input: {
      readonly policyId: string;
      readonly policy: Readonly<Record<string, unknown>>;
      readonly intent: "replace-replication-policy";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          policyId: Schema.NonEmptyString,
          policy: Native,
          intent: Schema.Literal("replace-replication-policy"),
        }),
        input,
        "setReplication",
        "azure",
        true,
      );
      return yield* call(
        "setReplication",
        "PUT",
        `/objectReplicationPolicies/${encodeURIComponent(value.policyId)}`,
        value.policy,
      );
    }),
  };
});
export class AzureAccountAdministration extends Context.Service<
  AzureAccountAdministration,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/azure/AzureAccountAdministration") {
  static readonly make = make;
  static layerHttp(config: AzureAccountConfig) {
    return Layer.effect(AzureAccountAdministration, make(config));
  }
  static layer(config: AzureAccountConfig) {
    return AzureAccountAdministration.layerHttp(config).pipe(Layer.provide(FetchHttpClient.layer));
  }
}
