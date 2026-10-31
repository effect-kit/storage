import { Context, Effect, Option, Schema, Stream } from "effect";
import type { DateTime, Duration } from "effect";

import type { StorageError } from "../core/errors.js";
import { storageError } from "../core/errors.js";
import {
  NativeVersion,
  ObjectHeaders,
  ObjectMetadata,
  ObjectRef,
  ObjectValidator,
} from "../core/object.js";
import { WriteCondition } from "../core/requests.js";

export const VersionRef = Schema.Struct({ ...ObjectRef.fields, version: NativeVersion });
export type VersionRef = typeof VersionRef.Type;
export const VersionEntry = Schema.Union([
  Schema.TaggedStruct("Version", {
    metadata: ObjectMetadata,
    isLatest: Schema.optional(Schema.Boolean),
    deleted: Schema.optional(Schema.Boolean),
    recoveryToken: Schema.optional(Schema.Redacted(Schema.NonEmptyString)),
  }),
  Schema.TaggedStruct("DeleteMarker", {
    ref: VersionRef,
    isLatest: Schema.optional(Schema.Boolean),
    createdAt: Schema.optional(Schema.DateTimeUtcFromString),
  }),
]);
export type VersionEntry = typeof VersionEntry.Type;
export const VersionQuery = Schema.Struct({
  prefix: Schema.optional(Schema.String),
  cursor: Schema.optional(Schema.NonEmptyString),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
  deleted: Schema.optional(Schema.Boolean),
});
export type VersionQuery = typeof VersionQuery.Type;
export interface VersionPage {
  readonly entries: ReadonlyArray<VersionEntry>;
  readonly cursor?: string | undefined;
}
export const RestoreVersion = Schema.Struct({
  source: VersionRef,
  destinationKey: Schema.NonEmptyString,
  condition: WriteCondition,
});
export interface VersionsService {
  readonly listPage: (query?: VersionQuery) => Effect.Effect<VersionPage, StorageError>;
  readonly list: (
    query?: Omit<VersionQuery, "cursor">,
  ) => Stream.Stream<VersionEntry, StorageError>;
  readonly deleteVersion: (input: {
    readonly ref: VersionRef;
  }) => Effect.Effect<void, StorageError>;
  readonly removeDeleteMarker: (input: {
    readonly ref: VersionRef;
  }) => Effect.Effect<void, StorageError>;
  readonly restore: (
    input: typeof RestoreVersion.Type,
  ) => Effect.Effect<ObjectMetadata, StorageError>;
}
export class StorageVersions extends Context.Service<StorageVersions, VersionsService>()(
  "@effect-kit/storage/StorageVersions",
) {
  static make(service: Omit<VersionsService, "list">): VersionsService {
    return {
      ...service,
      list: (query = {}) =>
        Stream.paginate(undefined as string | undefined, (cursor) =>
          service.listPage({ ...query, ...(cursor ? { cursor } : {}) }).pipe(
            Effect.flatMap((page) =>
              page.cursor && page.cursor === cursor
                ? Effect.fail(
                    storageError("versions.list", "storage", {
                      _tag: "ProviderFailure",
                      code: "RepeatedCursor",
                    }),
                  )
                : Effect.succeed([
                    page.entries,
                    page.cursor === undefined ? Option.none() : Option.some(page.cursor),
                  ] as const),
            ),
          ),
        ),
    };
  }
}

export const MetadataReplace = Schema.Struct({
  ref: ObjectRef,
  headers: ObjectHeaders,
  condition: Schema.optional(WriteCondition),
});
export const MetadataMerge = Schema.Struct({
  ref: ObjectRef,
  patch: Schema.Struct({
    ...ObjectHeaders.fields,
    customMetadata: Schema.optional(Schema.Record(Schema.String, Schema.NullOr(ObjectValidator))),
  }),
  condition: Schema.optional(WriteCondition),
});
export interface MetadataService {
  readonly replace: (
    input: typeof MetadataReplace.Type,
  ) => Effect.Effect<ObjectMetadata, StorageError>;
  readonly merge: (input: typeof MetadataMerge.Type) => Effect.Effect<ObjectMetadata, StorageError>;
}
export class StorageMetadata extends Context.Service<StorageMetadata, MetadataService>()(
  "@effect-kit/storage/StorageMetadata",
) {}
export const Tags = Schema.Record(Schema.NonEmptyString, Schema.String);
export const TagTarget = Schema.Struct({ ref: ObjectRef });
export const SetTags = Schema.Struct({ ref: ObjectRef, tags: Tags });
export interface TagsService {
  readonly get: (
    input: typeof TagTarget.Type,
  ) => Effect.Effect<Readonly<Record<string, string>>, StorageError>;
  readonly set: (input: typeof SetTags.Type) => Effect.Effect<void, StorageError>;
  readonly delete: (input: typeof TagTarget.Type) => Effect.Effect<void, StorageError>;
}
export class StorageTags extends Context.Service<StorageTags, TagsService>()(
  "@effect-kit/storage/StorageTags",
) {}

export type RestoreStatus =
  | { readonly _tag: "Archived" }
  | { readonly _tag: "Pending" }
  | { readonly _tag: "Ready"; readonly availableUntil?: DateTime.Utc | undefined };
export interface ArchiveService<Options> {
  readonly requestRestore: (input: {
    readonly ref: ObjectRef;
    readonly options: Options;
  }) => Effect.Effect<RestoreStatus, StorageError>;
  readonly restoreStatus: (input: {
    readonly ref: ObjectRef;
  }) => Effect.Effect<RestoreStatus, StorageError>;
  readonly awaitReady: (input: {
    readonly ref: ObjectRef;
    readonly timeout: Duration.Input;
    readonly pollInterval: Duration.Input;
  }) => Effect.Effect<RestoreStatus, StorageError>;
}
