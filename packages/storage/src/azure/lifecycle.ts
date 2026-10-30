import { Context, Effect, Layer, Schema } from "effect";

import { AzureStorage } from "../azure.js";
import type { AzureStorageService } from "../azure.js";
import { storageError } from "../errors.js";
import { decodeCursor, encodeCursor } from "../internal/listing-cursor.js";
import { decodeRequest } from "../internal/request-validation.js";
import {
  MetadataMerge,
  MetadataReplace,
  RestoreVersion,
  SetTags,
  StorageMetadata,
  StorageTags,
  StorageVersions,
  TagTarget,
  VersionQuery,
  VersionRef,
} from "../lifecycle.js";
import type { MetadataService, TagsService, VersionEntry, VersionsService } from "../lifecycle.js";
import { mergeHeaders, replaceHeaders } from "../lifecycle/metadata.js";
import { encryption, lease } from "./config.js";
import { condition, metadata, target, writeMetadata } from "./mapping.js";
import { request } from "./transport.js";

export class AzureVersions extends Context.Service<AzureVersions, VersionsService>()(
  "@effect-kit/storage/azure/AzureVersions",
) {
  static make(storage: AzureStorageService): VersionsService {
    return StorageVersions.make({
      listPage: Effect.fnUntraced(function* (input = {}) {
        const value = yield* decodeRequest(VersionQuery, input, "listVersions", "azure");
        const identity = JSON.stringify(["versions", value.prefix, value.limit, value.deleted]);
        const token = yield* decodeCursor(
          "azure",
          value.cursor,
          storage.storeId,
          identity,
          "listVersions",
        );
        const page = yield* request(storage, "listVersions", false, (signal) =>
          storage.client
            .listBlobsFlat({
              ...(value.prefix ? { prefix: value.prefix } : {}),
              includeVersions: true,
              includeSnapshots: true,
              includeDeleted: value.deleted ?? false,
              abortSignal: signal,
            })
            .byPage({
              ...(token ? { continuationToken: token } : {}),
              maxPageSize: value.limit ?? 1000,
            })
            .next(),
        );
        const entries: VersionEntry[] = [];
        for (const blob of page.value?.segment.blobItems ?? []) {
          const version = blob.snapshot
            ? { _tag: "AzureSnapshot" as const, id: blob.snapshot }
            : blob.versionId
              ? { _tag: "AzureVersion" as const, id: blob.versionId }
              : undefined;
          if (!version) continue;
          entries.push({
            _tag: "Version",
            deleted: blob.deleted,
            metadata: metadata(
              storage,
              blob.name,
              { ...blob.properties, metadata: blob.metadata },
              { storeId: storage.storeId, key: blob.name, version },
            ),
            ...(blob.isCurrentVersion !== undefined ? { isLatest: blob.isCurrentVersion } : {}),
          });
        }
        const next = page.value?.continuationToken;
        return {
          entries,
          ...(next ? { cursor: encodeCursor(storage.storeId, identity, next) } : {}),
        };
      }),
      deleteVersion: Effect.fnUntraced(function* (input) {
        const { ref } = yield* decodeRequest(
          Schema.Struct({ ref: VersionRef }),
          input,
          "deleteVersion",
          "azure",
          true,
        );
        const address = yield* target(storage, { ref }, "deleteVersion");
        yield* request(storage, "deleteVersion", true, (signal) =>
          address.client.delete({ abortSignal: signal, conditions: lease(storage.config) }),
        );
      }),
      removeDeleteMarker: () =>
        Effect.fail(
          storageError(
            "removeDeleteMarker",
            "azure",
            { _tag: "Unsupported", capability: "s3-delete-markers" },
            "not-started",
          ),
        ),
      restore: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(RestoreVersion, input, "restoreVersion", "azure", true);
        const source = yield* target(storage, { ref: value.source }, "restoreVersion");
        const destination = yield* target(storage, { key: value.destinationKey }, "restoreVersion");
        const output = yield* request(storage, "restoreVersion", true, (signal) =>
          destination.client.syncCopyFromURL(source.client.url, {
            abortSignal: signal,
            conditions: { ...condition(value.condition), ...lease(storage.config) },
          }),
        );
        if (!output.etag)
          return yield* storageError(
            "restoreVersion",
            "azure",
            { _tag: "ProviderFailure", code: "MissingCopyReceipt" },
            "unknown",
          );
        return metadata(storage, value.destinationKey, output);
      }),
    });
  }
  static readonly layer = Layer.effectContext(
    Effect.map(AzureStorage, (storage) => {
      const service = AzureVersions.make(storage);
      return Context.make(AzureVersions, service).pipe(Context.add(StorageVersions, service));
    }),
  );
}
export class AzureMetadata extends Context.Service<AzureMetadata, MetadataService>()(
  "@effect-kit/storage/azure/AzureMetadata",
) {
  static make(storage: AzureStorageService): MetadataService {
    const update = Effect.fnUntraced(function* (
      input: typeof MetadataReplace.Type,
      patch?: (typeof MetadataMerge.Type)["patch"],
    ) {
      const address = yield* target(storage, { ref: input.ref }, "updateMetadata");
      const proposed = patch ?? input.headers;
      if (
        input.ref.version ||
        !input.condition ||
        input.condition._tag !== "IfMatch" ||
        [
          proposed.contentType,
          proposed.contentEncoding,
          proposed.contentLanguage,
          proposed.contentDisposition,
          proposed.cacheControl,
        ].some((field) => field !== undefined)
      )
        return yield* storageError(
          "updateMetadata",
          "azure",
          { _tag: "Unsupported", capability: "current-if-match-custom-metadata-only" },
          "not-started",
        );
      const conditions = { ifMatch: input.condition.etag, ...lease(storage.config) };
      const before = yield* request(storage, "updateMetadata.head", false, (signal) =>
        address.client.getProperties({
          abortSignal: signal,
          conditions,
          ...encryption(storage.config),
        }),
      );
      const previous = metadata(storage, input.ref.key, before);
      const headers = patch
        ? mergeHeaders(previous, patch)
        : replaceHeaders(previous, input.headers);
      const native = yield* writeMetadata(headers);
      const output = yield* request(storage, "updateMetadata", true, (signal) =>
        address.client.setMetadata(native, {
          abortSignal: signal,
          conditions,
          ...encryption(storage.config),
        }),
      );
      if (!output.etag)
        return yield* storageError(
          "updateMetadata",
          "azure",
          { _tag: "ProviderFailure", code: "MissingMetadataReceipt" },
          "unknown",
        );
      return {
        ...previous,
        ...metadata(storage, input.ref.key, {
          ...before,
          versionId: undefined,
          ...output,
          metadata: native,
        }),
      };
    });
    return {
      replace: (input) =>
        decodeRequest(MetadataReplace, input, "updateMetadata", "azure", true).pipe(
          Effect.flatMap((value) => update(value)),
        ),
      merge: (input) =>
        decodeRequest(MetadataMerge, input, "updateMetadata", "azure", true).pipe(
          Effect.flatMap((value) =>
            update({ ref: value.ref, headers: {}, condition: value.condition }, value.patch),
          ),
        ),
    };
  }
  static readonly layer = Layer.effectContext(
    Effect.map(AzureStorage, (storage) => {
      const service = AzureMetadata.make(storage);
      return Context.make(AzureMetadata, service).pipe(Context.add(StorageMetadata, service));
    }),
  );
}
export class AzureTags extends Context.Service<AzureTags, TagsService>()(
  "@effect-kit/storage/azure/AzureTags",
) {
  static make(storage: AzureStorageService): TagsService {
    const set = Effect.fnUntraced(function* (input: typeof SetTags.Type) {
      const value = yield* decodeRequest(SetTags, input, "setTags", "azure", true);
      if (
        Object.keys(value.tags).length > 10 ||
        Object.entries(value.tags).some(
          ([key, tagValue]) =>
            !/^[A-Za-z0-9 +\-.:=_/]{1,128}$/u.test(key) ||
            !/^[A-Za-z0-9 +\-.:=_/]{0,256}$/u.test(tagValue),
        )
      )
        return yield* storageError(
          "setTags",
          "azure",
          { _tag: "InvalidInput", field: "tags" },
          "not-started",
        );
      const address = yield* target(storage, { ref: value.ref }, "setTags");
      yield* request(storage, "setTags", true, (signal) =>
        address.client.setTags({ ...value.tags }, { abortSignal: signal }),
      );
    });
    return {
      set,
      delete: (input) =>
        decodeRequest(TagTarget, input, "deleteTags", "azure", true).pipe(
          Effect.flatMap((value) => set({ ...value, tags: {} })),
        ),
      get: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(TagTarget, input, "getTags", "azure");
        const address = yield* target(storage, { ref: value.ref }, "getTags");
        return (yield* request(storage, "getTags", false, (signal) =>
          address.client.getTags({ abortSignal: signal }),
        )).tags;
      }),
    };
  }
  static readonly layer = Layer.effectContext(
    Effect.map(AzureStorage, (storage) => {
      const service = AzureTags.make(storage);
      return Context.make(AzureTags, service).pipe(Context.add(StorageTags, service));
    }),
  );
}

export { AzureProperties } from "./properties.js";
