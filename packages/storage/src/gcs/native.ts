import { Effect, Schema } from "effect";

import { storageError } from "../errors.js";
import type { ObjectHeaders, ObjectRef } from "../object.js";
import type { GcsConnection } from "./config.js";
import { metadata, nativeHeaders, objectPath, target } from "./mapping.js";
import { decodeJson, jsonRequest } from "./transport.js";
import { validateKey } from "./validation.js";

export interface GcsPreconditions {
  readonly ifGenerationMatch?: string;
  readonly ifGenerationNotMatch?: string;
  readonly ifMetagenerationMatch?: string;
  readonly ifMetagenerationNotMatch?: string;
}
export const preconditions = (value: GcsPreconditions) =>
  Schema.decodeUnknownEffect(
    Schema.Struct({
      ifGenerationMatch: Schema.optional(Schema.String.check(Schema.isPattern(/^\d+$/u))),
      ifGenerationNotMatch: Schema.optional(Schema.String.check(Schema.isPattern(/^\d+$/u))),
      ifMetagenerationMatch: Schema.optional(Schema.String.check(Schema.isPattern(/^\d+$/u))),
      ifMetagenerationNotMatch: Schema.optional(Schema.String.check(Schema.isPattern(/^\d+$/u))),
    }),
    { onExcessProperty: "error" },
  )(value).pipe(
    Effect.mapError(() =>
      storageError(
        "preconditions",
        "gcs",
        { _tag: "InvalidInput", field: "preconditions" },
        "not-started",
      ),
    ),
  );

export function nativeOperations(connection: GcsConnection) {
  return {
    updateMetadata: Effect.fnUntraced(function* (request: {
      readonly ref: ObjectRef;
      readonly headers: ObjectHeaders;
      readonly preconditions?: GcsPreconditions;
    }) {
      const address = yield* target(connection, { ref: request.ref }, "updateMetadata");
      const conditions = yield* preconditions(request.preconditions ?? {});
      const response = yield* jsonRequest(connection, "updateMetadata", true, {
        uri: address.uri,
        method: "PATCH",
        qs: { ...address.qs, ...conditions },
        json: nativeHeaders(request.headers),
      });
      return yield* metadata(connection, response.body, "updateMetadata");
    }),
    deleteGeneration: Effect.fnUntraced(function* (request: {
      readonly ref: ObjectRef;
      readonly preconditions?: GcsPreconditions;
    }) {
      const address = yield* target(connection, { ref: request.ref }, "delete");
      const conditions = yield* preconditions(request.preconditions ?? {});
      yield* jsonRequest(connection, "delete", true, {
        uri: address.uri,
        method: "DELETE",
        qs: { ...address.qs, ...conditions },
      });
    }),
    rewrite: Effect.fnUntraced(function* (request: {
      readonly source: ObjectRef;
      readonly destinationKey: string;
      readonly headers?: ObjectHeaders;
      readonly preconditions?: GcsPreconditions;
      readonly token?: string;
      readonly storageClass?: "STANDARD" | "NEARLINE" | "COLDLINE" | "ARCHIVE";
    }) {
      yield* validateKey(request.destinationKey, "rewrite", true);
      const source = yield* target(connection, { ref: request.source }, "rewrite");
      if (!source.ref.version)
        return yield* storageError(
          "rewrite",
          "gcs",
          { _tag: "InvalidInput", field: "source.generation" },
          "not-started",
        );
      const conditions = yield* preconditions(request.preconditions ?? {});
      const response = yield* jsonRequest(connection, "rewrite", true, {
        uri: `${source.uri}/rewriteTo/b/${encodeURIComponent(connection.config.bucket)}/o/${encodeURIComponent(request.destinationKey)}`,
        method: "POST",
        qs: {
          sourceGeneration: source.ref.version.id,
          ...conditions,
          rewriteToken: request.token,
          ...(connection.config.kmsKeyName
            ? { destinationKmsKeyName: connection.config.kmsKeyName }
            : {}),
        },
        json: {
          ...nativeHeaders(request.headers ?? {}),
          ...(request.storageClass ? { storageClass: request.storageClass } : {}),
        },
      });
      const result = yield* decodeJson(
        Schema.Struct({
          done: Schema.Boolean,
          rewriteToken: Schema.optional(Schema.String),
          totalBytesRewritten: Schema.String,
          objectSize: Schema.String,
          resource: Schema.optional(Schema.Unknown),
        }),
        response.body,
        "rewrite",
      );
      if (!result.done && !result.rewriteToken)
        return yield* storageError("rewrite", "gcs", { _tag: "ProviderFailure" }, "unknown");
      return result.done
        ? {
            _tag: "Completed" as const,
            metadata: yield* metadata(connection, result.resource, "rewrite"),
          }
        : {
            _tag: "Pending" as const,
            token: result.rewriteToken,
            bytesRewritten: result.totalBytesRewritten,
            objectSize: result.objectSize,
          };
    }),
    compose: Effect.fnUntraced(function* (request: {
      readonly sources: ReadonlyArray<ObjectRef>;
      readonly destinationKey: string;
      readonly headers?: ObjectHeaders;
      readonly preconditions?: GcsPreconditions;
    }) {
      yield* validateKey(request.destinationKey, "compose", true);
      if (request.sources.length < 1 || request.sources.length > 32)
        return yield* storageError(
          "compose",
          "gcs",
          { _tag: "InvalidInput", field: "sources" },
          "not-started",
        );
      const sources = yield* Effect.forEach(request.sources, (ref) =>
        target(connection, { ref }, "compose"),
      );
      if (sources.some(({ ref }) => !ref.version))
        return yield* storageError(
          "compose",
          "gcs",
          { _tag: "InvalidInput", field: "sources.generation" },
          "not-started",
        );
      if (
        request.preconditions?.ifGenerationNotMatch !== undefined ||
        request.preconditions?.ifMetagenerationNotMatch !== undefined
      )
        return yield* storageError(
          "compose",
          "gcs",
          { _tag: "Unsupported", capability: "compose-negative-preconditions" },
          "not-started",
        );
      const conditions = yield* preconditions(request.preconditions ?? {});
      const response = yield* jsonRequest(connection, "compose", true, {
        uri: `${connection.config.apiEndpoint}${objectPath(connection, request.destinationKey)}/compose`,
        method: "POST",
        qs: {
          ...conditions,
          ...(connection.config.kmsKeyName ? { kmsKeyName: connection.config.kmsKeyName } : {}),
        },
        json: {
          sourceObjects: sources.map(({ ref }) => ({ name: ref.key, generation: ref.version?.id })),
          destination: nativeHeaders(request.headers ?? {}),
        },
      });
      return yield* metadata(connection, response.body, "compose");
    }),
  };
}
