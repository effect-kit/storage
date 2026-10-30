import { Context, Effect, Layer, Redacted, Schema } from "effect";

import { storageError } from "../errors.js";
import { GcsStorage } from "../gcs.js";
import type { GcsStorageService } from "../gcs.js";
import { decodeCursor, encodeCursor } from "../internal/listing-cursor.js";
import { decodeRequest } from "../internal/request-validation.js";
import {
  MetadataMerge,
  MetadataReplace,
  RestoreVersion,
  StorageMetadata,
  StorageVersions,
  VersionQuery,
  VersionRef,
} from "../lifecycle.js";
import type { MetadataService, VersionEntry, VersionsService } from "../lifecycle.js";
import { mergeHeaders, replaceHeaders } from "../lifecycle/metadata.js";
import { metadata, nativeHeaders, objectPath, target } from "./mapping.js";
import { decodeJson, jsonRequest } from "./transport.js";

export class GcsVersions extends Context.Service<GcsVersions, VersionsService>()(
  "@effect-kit/storage/gcs/GcsVersions",
) {
  static make(storage: GcsStorageService): VersionsService {
    return StorageVersions.make({
      listPage: Effect.fnUntraced(function* (input = {}) {
        const value = yield* decodeRequest(VersionQuery, input, "listVersions", "gcs");
        const identity = JSON.stringify(["versions", value.prefix, value.limit, value.deleted]);
        const token = yield* decodeCursor(
          "gcs",
          value.cursor,
          storage.storeId,
          identity,
          "listVersions",
        );
        const response = yield* jsonRequest(storage, "listVersions", false, {
          uri: `${storage.config.apiEndpoint}/storage/v1/b/${encodeURIComponent(storage.config.bucket)}/o`,
          qs: {
            prefix: value.prefix,
            maxResults: value.limit ?? 1000,
            pageToken: token,
            ...(value.deleted ? { softDeleted: "true" } : { versions: "true" }),
          },
          json: true,
        });
        const page = yield* decodeJson(
          Schema.Struct({
            items: Schema.optional(Schema.Array(Schema.Unknown)),
            nextPageToken: Schema.optional(Schema.String),
          }),
          response.body,
          "listVersions",
        );
        const entries: VersionEntry[] = yield* Effect.forEach(page.items ?? [], (raw) =>
          Effect.gen(function* () {
            const observed = yield* metadata(storage, raw, "listVersions");
            const recovery = yield* decodeJson(
              Schema.Struct({ restoreToken: Schema.optional(Schema.String) }),
              raw,
              "listVersions",
            );
            return {
              _tag: "Version" as const,
              metadata: observed,
              deleted: value.deleted ?? false,
              ...(recovery.restoreToken
                ? { recoveryToken: Redacted.make(recovery.restoreToken) }
                : {}),
            };
          }),
        );
        return {
          entries,
          ...(page.nextPageToken
            ? { cursor: encodeCursor(storage.storeId, identity, page.nextPageToken) }
            : {}),
        };
      }),
      deleteVersion: Effect.fnUntraced(function* (input) {
        const { ref } = yield* decodeRequest(
          Schema.Struct({ ref: VersionRef }),
          input,
          "deleteVersion",
          "gcs",
          true,
        );
        const address = yield* target(storage, { ref }, "delete");
        yield* jsonRequest(storage, "deleteVersion", true, {
          uri: address.uri,
          method: "DELETE",
          qs: { ...address.qs, ifGenerationMatch: ref.version.id },
          json: true,
        });
      }),
      removeDeleteMarker: () =>
        Effect.fail(
          storageError(
            "removeDeleteMarker",
            "gcs",
            { _tag: "Unsupported", capability: "s3-delete-markers" },
            "not-started",
          ),
        ),
      restore: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(RestoreVersion, input, "restoreVersion", "gcs", true);
        yield* target(storage, { ref: value.source }, "restoreVersion");
        if (value.condition._tag !== "CreateOnly")
          return yield* storageError(
            "restoreVersion",
            "gcs",
            { _tag: "Unsupported", capability: "etag-destination-generation-condition" },
            "not-started",
          );
        let token: string | undefined;
        for (let attempt = 0; attempt < 1000; attempt++) {
          const result = yield* storage.rewrite({
            source: value.source,
            destinationKey: value.destinationKey,
            preconditions: { ifGenerationMatch: "0" },
            ...(token ? { token } : {}),
          });
          if (result._tag === "Completed") return result.metadata;
          token = result.token;
        }
        return yield* storageError(
          "restoreVersion",
          "gcs",
          { _tag: "TransportFailure", code: "RewriteBudgetExceeded" },
          "unknown",
        );
      }),
    });
  }
  static readonly layer = Layer.effectContext(
    Effect.map(GcsStorage, (storage) => {
      const service = GcsVersions.make(storage);
      return Context.make(GcsVersions, service).pipe(Context.add(StorageVersions, service));
    }),
  );
}
export class GcsMetadata extends Context.Service<GcsMetadata, MetadataService>()(
  "@effect-kit/storage/gcs/GcsMetadata",
) {
  static make(storage: GcsStorageService): MetadataService {
    const update = Effect.fnUntraced(function* (
      input: typeof MetadataReplace.Type,
      patch?: (typeof MetadataMerge.Type)["patch"],
    ) {
      const address = yield* target(storage, { ref: input.ref }, "updateMetadata");
      if (input.condition)
        return yield* storageError(
          "updateMetadata",
          "gcs",
          { _tag: "Unsupported", capability: "etag-metadata-write-condition" },
          "not-started",
        );
      if (input.ref.version?._tag !== "GcsGeneration" || !input.ref.version.metageneration)
        return yield* storageError(
          "updateMetadata",
          "gcs",
          { _tag: "InvalidInput", field: "generation-and-metageneration" },
          "not-started",
        );
      const current = yield* storage.head({ ref: input.ref });
      const headers = patch ? mergeHeaders(current, patch) : replaceHeaders(current, input.headers);
      const body = nativeHeaders(headers);
      const response = yield* jsonRequest(storage, "updateMetadata", true, {
        uri: address.uri,
        method: "PATCH",
        qs: {
          ...address.qs,
          ifGenerationMatch: input.ref.version.id,
          ifMetagenerationMatch: input.ref.version.metageneration,
        },
        json: {
          contentType: body.contentType ?? null,
          contentEncoding: body.contentEncoding ?? null,
          contentLanguage: body.contentLanguage ?? null,
          contentDisposition: body.contentDisposition ?? null,
          cacheControl: body.cacheControl ?? null,
          metadata: body.metadata ?? null,
        },
      });
      return yield* metadata(storage, response.body, "updateMetadata");
    });
    return {
      replace: (input) =>
        decodeRequest(MetadataReplace, input, "updateMetadata", "gcs", true).pipe(
          Effect.flatMap((value) => update(value)),
        ),
      merge: (input) =>
        decodeRequest(MetadataMerge, input, "updateMetadata", "gcs", true).pipe(
          Effect.flatMap((value) =>
            update({ ref: value.ref, headers: {}, condition: value.condition }, value.patch),
          ),
        ),
    };
  }
  static readonly layer = Layer.effectContext(
    Effect.map(GcsStorage, (storage) => {
      const service = GcsMetadata.make(storage);
      return Context.make(GcsMetadata, service).pipe(Context.add(StorageMetadata, service));
    }),
  );
}
const RecoveryRequest = Schema.Struct({
  ref: VersionRef,
  restoreToken: Schema.optional(Schema.NonEmptyString),
  copySourceAcl: Schema.optional(Schema.Boolean),
});
const recovery = (storage: GcsStorageService) => ({
  restoreDeleted: Effect.fnUntraced(function* (input: typeof RecoveryRequest.Type) {
    const value = yield* decodeRequest(RecoveryRequest, input, "restoreDeleted", "gcs", true);
    const address = yield* target(storage, { ref: value.ref }, "restoreDeleted");
    const response = yield* jsonRequest(storage, "restoreDeleted", true, {
      uri: `${storage.config.apiEndpoint}${objectPath(storage, value.ref.key)}/restore`,
      method: "POST",
      qs: {
        ...address.qs,
        restoreToken: value.restoreToken,
        copySourceAcl: value.copySourceAcl === undefined ? undefined : String(value.copySourceAcl),
        ifGenerationMatch: "0",
      },
      json: {},
    });
    return yield* metadata(storage, response.body, "restoreDeleted");
  }),
});
export class GcsRecovery extends Context.Service<GcsRecovery, ReturnType<typeof recovery>>()(
  "@effect-kit/storage/gcs/GcsRecovery",
) {
  static readonly make = recovery;
  static readonly layer = Layer.effect(GcsRecovery, Effect.map(GcsStorage, recovery));
}
