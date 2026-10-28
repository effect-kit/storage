import { Schema } from "effect";
import type { Scope, Stream } from "effect";

import type { StorageError } from "./errors.js";
import type { ObjectMetadata } from "./object.js";
import { ObjectHeaders, ObjectKey, ObjectRef, ObjectSize, ObjectValidator } from "./object.js";
import { ByteRange } from "./ranges.js";
import type { ResolvedRange } from "./ranges.js";

export const WriteCondition = Schema.Union([
  Schema.TaggedStruct("CreateOnly", {}),
  Schema.TaggedStruct("IfMatch", { etag: ObjectValidator }),
]);

export type WriteCondition = typeof WriteCondition.Type;

export const ReadCondition = Schema.Union([
  Schema.TaggedStruct("IfMatch", { etag: ObjectValidator }),
  Schema.TaggedStruct("IfNoneMatch", { etag: ObjectValidator }),
]);

export type ReadCondition = typeof ReadCondition.Type;

export type UploadBody<E = never, R = never> = Uint8Array | Stream.Stream<Uint8Array, E, R>;

export const PutRequest = Schema.Struct({
  ...ObjectHeaders.fields,
  key: ObjectKey,
  body: Schema.Unknown,
  condition: Schema.optional(WriteCondition),
  contentLength: Schema.optional(ObjectSize),
  maxBytes: Schema.optional(ObjectSize),
});

export type PutRequest<E = never, R = never> = Omit<typeof PutRequest.Type, "body"> & {
  readonly body: UploadBody<E, R>;
};

const Target = Schema.Union([Schema.Struct({ key: ObjectKey }), Schema.Struct({ ref: ObjectRef })]);

export type ObjectTarget = typeof Target.Type;
export const HeadRequest = Target;
export type HeadRequest = ObjectTarget;

export const GetRequest = Schema.Union([
  Schema.Struct({
    key: ObjectKey,
    range: Schema.optional(ByteRange),
    condition: Schema.optional(ReadCondition),
  }),
  Schema.Struct({
    ref: ObjectRef,
    range: Schema.optional(ByteRange),
    condition: Schema.optional(ReadCondition),
  }),
]);

export type GetRequest = typeof GetRequest.Type;

export const DeleteRequest = Schema.Union([
  Schema.Struct({
    key: ObjectKey,
    condition: Schema.optional(Schema.TaggedStruct("IfMatch", { etag: ObjectValidator })),
  }),
  Schema.Struct({
    ref: ObjectRef,
    condition: Schema.optional(Schema.TaggedStruct("IfMatch", { etag: ObjectValidator })),
  }),
]);

export type DeleteRequest = typeof DeleteRequest.Type;

export const ListRequest = Schema.Struct({
  prefix: Schema.optional(Schema.String),
  delimiter: Schema.optional(Schema.NonEmptyString),
  limit: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
  cursor: Schema.optional(Schema.NonEmptyString),
});

export type ListRequest = typeof ListRequest.Type;

export interface ObjectDownload {
  readonly _tag: "Object";
  readonly metadata: ObjectMetadata;
  readonly contentLength?: number | undefined;
  readonly range?: ResolvedRange | undefined;
  readonly body: Stream.Stream<Uint8Array, StorageError, Scope.Scope>;
}

export interface NotModified {
  readonly _tag: "NotModified";
  readonly metadata: ObjectMetadata;
}

export type GetResult = ObjectDownload | NotModified;

export type ListEntry =
  | { readonly _tag: "Object"; readonly metadata: ObjectMetadata }
  | { readonly _tag: "CommonPrefix"; readonly prefix: string };

export interface ListPage {
  readonly entries: ReadonlyArray<ListEntry>;
  readonly cursor?: string | undefined;
}
