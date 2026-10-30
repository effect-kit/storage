import { Context, Effect, Layer, Redacted, Schema } from "effect";

import { BlobServiceClient } from "@azure/storage-blob";
import type { BlobServiceProperties } from "@azure/storage-blob";

import { storageError } from "../errors.js";
import { controlEndpoint } from "../internal/http/control-plane.js";
import { providerError } from "../internal/node/provider-errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import type { AzureConfig } from "./config.js";

export interface AzureAdminConfig {
  readonly serviceUrl: string | Redacted.Redacted<string>;
  readonly credential?: AzureConfig["credential"];
}
const Container = Schema.String.check(
  Schema.isPattern(/^(?!.*--)[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/u),
);
const call = <A>(
  operation: string,
  mutation: boolean,
  request: (signal: AbortSignal) => Promise<A>,
) =>
  Effect.tryPromise({
    try: request,
    catch: (error) => providerError("azure", operation, mutation, error),
  }).pipe(
    Effect.timeoutOrElse({
      duration: "30 seconds",
      orElse: () => Effect.fail(providerError("azure", operation, mutation, undefined)),
    }),
  );

const make = Effect.fnUntraced(function* (config: AzureAdminConfig) {
  const url = yield* Effect.try({
    try: () =>
      new URL(
        Redacted.isRedacted(config.serviceUrl)
          ? Redacted.value(config.serviceUrl)
          : config.serviceUrl,
      ),
    catch: () =>
      storageError("makeAdministration", "azure", {
        _tag: "InvalidConfiguration",
        field: "serviceUrl",
      }),
  });
  yield* controlEndpoint(url.origin, "azure");
  if (
    url.username ||
    url.password ||
    url.hash ||
    (!config.credential && (!Redacted.isRedacted(config.serviceUrl) || !url.search))
  )
    return yield* storageError("makeAdministration", "azure", {
      _tag: "InvalidConfiguration",
      field: "management-credential",
    });
  const client = yield* Effect.try({
    try: () =>
      new BlobServiceClient(url.href, config.credential, { retryOptions: { maxTries: 1 } }),
    catch: () =>
      storageError("makeAdministration", "azure", {
        _tag: "InvalidConfiguration",
        field: "client",
      }),
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
        "listContainers",
        "azure",
      );
      const page = yield* call("listContainers", false, (signal) =>
        client
          .listContainers({ abortSignal: signal })
          .byPage({
            ...(value.cursor ? { continuationToken: value.cursor } : {}),
            maxPageSize: value.limit ?? 1000,
          })
          .next(),
      );
      return {
        containers: page.value?.containerItems ?? [],
        cursor: page.value?.continuationToken,
      };
    }),
    head: Effect.fnUntraced(function* (input: { readonly container: string }) {
      const value = yield* decodeRequest(
        Schema.Struct({ container: Container }),
        input,
        "headContainer",
        "azure",
      );
      return yield* call("headContainer", false, (signal) =>
        client.getContainerClient(value.container).getProperties({ abortSignal: signal }),
      );
    }),
    create: Effect.fnUntraced(function* (input: { readonly container: string }) {
      const value = yield* decodeRequest(
        Schema.Struct({ container: Container }),
        input,
        "createContainer",
        "azure",
        true,
      );
      yield* call("createContainer", true, (signal) =>
        client.getContainerClient(value.container).create({ abortSignal: signal }),
      );
      return { _tag: "Accepted" as const };
    }),
    deleteEmpty: (_input: { readonly container: string }) =>
      Effect.fail(
        storageError(
          "deleteEmpty",
          "azure",
          { _tag: "Unsupported", capability: "atomic-nonempty-container-protection" },
          "not-started",
        ),
      ),
    configuration: {
      get: () =>
        call("getServiceProperties", false, (signal) =>
          client.getProperties({ abortSignal: signal }),
        ),
      set: (input: {
        readonly properties: BlobServiceProperties;
        readonly intent: "replace-service-configuration";
      }) =>
        input.intent === "replace-service-configuration"
          ? call("setServiceProperties", true, (signal) =>
              client.setProperties(input.properties, { abortSignal: signal }),
            )
          : Effect.fail(
              storageError(
                "setServiceProperties",
                "azure",
                { _tag: "InvalidInput", field: "intent" },
                "not-started",
              ),
            ),
    },
  };
});
export class AzureAdministration extends Context.Service<
  AzureAdministration,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/azure/AzureAdministration") {
  static readonly make = make;
  static layer(config: AzureAdminConfig) {
    return Layer.effect(AzureAdministration, make(config));
  }
}
