import { Context, Effect, Layer, Schema } from "effect";

import { DeleteObjectCommand, ListObjectVersionsCommand } from "@aws-sdk/client-s3";

import { storageError } from "../../core/errors.js";
import { decodeCursor, encodeCursor } from "../../internal/listing-cursor.js";
import { decodeRequest } from "../../internal/request-validation.js";
import {
  RestoreVersion,
  StorageVersions,
  VersionQuery,
  VersionRef,
} from "../../lifecycle/index.js";
import type { VersionEntry, VersionsService } from "../../lifecycle/index.js";
import { S3Storage } from "./index.js";
import type { S3StorageService } from "./index.js";
import { lifecycleFeature, lifecycleTarget } from "./lifecycle-validation.js";
import { metadata } from "./object-mapping.js";
import { request } from "./transport.js";

export class S3Versions extends Context.Service<S3Versions, VersionsService>()(
  "@effect-kit/storage/s3/S3Versions",
) {
  static make(storage: S3StorageService): VersionsService {
    const remove = Effect.fnUntraced(function* (
      input: { readonly ref: VersionRef },
      marker: boolean,
    ) {
      const { ref } = yield* decodeRequest(
        Schema.Struct({ ref: VersionRef }),
        input,
        "deleteVersion",
        storage.provider,
        true,
      );
      yield* lifecycleFeature(storage, "versions");
      const target = yield* lifecycleTarget(storage, ref, "deleteVersion", true);
      let keyMarker: string | undefined;
      let versionMarker: string | undefined;
      let found = false;
      for (let pageNumber = 0; pageNumber < 1000; pageNumber++) {
        const page = yield* request(storage, "inspectVersionKind", false, (signal) =>
          storage.client.send(
            new ListObjectVersionsCommand({
              Bucket: target.Bucket,
              Prefix: ref.key,
              MaxKeys: 1000,
              KeyMarker: keyMarker,
              VersionIdMarker: versionMarker,
            }),
            { abortSignal: signal },
          ),
        );
        const content = page.Versions?.some(
          (version) => version.Key === ref.key && version.VersionId === ref.version.id,
        );
        const deleted = page.DeleteMarkers?.some(
          (version) => version.Key === ref.key && version.VersionId === ref.version.id,
        );
        if (content || deleted) {
          if (marker !== Boolean(deleted))
            return yield* storageError(
              "deleteVersion",
              storage.provider,
              { _tag: "InvalidInput", field: "version-kind" },
              "not-started",
            );
          found = true;
          break;
        }
        if (!page.IsTruncated) break;
        if (
          !page.NextKeyMarker ||
          !page.NextVersionIdMarker ||
          (page.NextKeyMarker === keyMarker && page.NextVersionIdMarker === versionMarker)
        )
          return yield* storageError(
            "inspectVersionKind",
            storage.provider,
            { _tag: "ProviderFailure", code: "InvalidContinuation" },
            "not-started",
          );
        keyMarker = page.NextKeyMarker;
        versionMarker = page.NextVersionIdMarker;
      }
      if (!found)
        return yield* storageError(
          "deleteVersion",
          storage.provider,
          { _tag: "NotFound" },
          "not-started",
        );
      yield* request(storage, "deleteVersion", true, (signal) =>
        storage.client.send(new DeleteObjectCommand(target), { abortSignal: signal }),
      );
    });
    return StorageVersions.make({
      deleteVersion: (input) => remove(input, false),
      removeDeleteMarker: (input) => remove(input, true),
      listPage: Effect.fnUntraced(function* (input = {}) {
        const query = yield* decodeRequest(VersionQuery, input, "listVersions", storage.provider);
        yield* lifecycleFeature(storage, "versions");
        if (query.deleted)
          return yield* storageError("listVersions", storage.provider, {
            _tag: "Unsupported",
            capability: "soft-delete-list",
          });
        const identity = JSON.stringify([
          "versions",
          storage.profile?.configuration,
          query.prefix ?? "",
          query.limit ?? 1000,
        ]);
        const token = yield* decodeCursor(
          storage.provider,
          query.cursor,
          storage.storeId,
          identity,
          "listVersions",
        );
        const marker = token
          ? yield* decodeRequest(
              Schema.fromJsonString(Schema.Struct({ key: Schema.String, version: Schema.String })),
              token,
              "listVersions",
              storage.provider,
            )
          : undefined;
        const page = yield* request(storage, "listVersions", false, (signal) =>
          storage.client.send(
            new ListObjectVersionsCommand({
              Bucket: storage.config.bucket,
              Prefix: query.prefix,
              MaxKeys: query.limit ?? 1000,
              KeyMarker: marker?.key,
              VersionIdMarker: marker?.version,
            }),
            { abortSignal: signal },
          ),
        );
        const entries: VersionEntry[] = [];
        for (const version of page.Versions ?? []) {
          if (!version.Key || !version.VersionId)
            return yield* storageError("listVersions", storage.provider, {
              _tag: "ProviderFailure",
              code: "InvalidVersionInventory",
            });
          entries.push({
            _tag: "Version",
            metadata: metadata(storage.config, version.Key, {
              ...version,
              ContentLength: version.Size,
            }),
            isLatest: version.IsLatest ?? false,
          });
        }
        for (const removed of page.DeleteMarkers ?? []) {
          if (!removed.Key || !removed.VersionId)
            return yield* storageError("listVersions", storage.provider, {
              _tag: "ProviderFailure",
              code: "InvalidMarkerInventory",
            });
          entries.push({
            _tag: "DeleteMarker",
            ref: {
              storeId: storage.storeId,
              key: removed.Key,
              version: { _tag: "S3Version", id: removed.VersionId },
            },
            isLatest: removed.IsLatest ?? false,
          });
        }
        if (page.IsTruncated && (!page.NextKeyMarker || !page.NextVersionIdMarker))
          return yield* storageError("listVersions", storage.provider, {
            _tag: "ProviderFailure",
            code: "MissingContinuation",
          });
        return {
          entries,
          ...(page.IsTruncated
            ? {
                cursor: encodeCursor(
                  storage.storeId,
                  identity,
                  JSON.stringify({ key: page.NextKeyMarker, version: page.NextVersionIdMarker }),
                ),
              }
            : {}),
        };
      }),
      restore: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(
          RestoreVersion,
          input,
          "restoreVersion",
          storage.provider,
          true,
        );
        yield* lifecycleFeature(storage, "versions");
        yield* lifecycleTarget(storage, value.source, "restoreVersion", true);
        return yield* storage.copyObject({
          source: value.source,
          destinationKey: value.destinationKey,
          condition: value.condition,
        });
      }),
    });
  }
  static readonly layer = Layer.effectContext(
    Effect.map(S3Storage, (storage) => {
      const service = S3Versions.make(storage);
      return Context.make(S3Versions, service).pipe(Context.add(StorageVersions, service));
    }),
  );
}
