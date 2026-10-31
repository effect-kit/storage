import { Schema } from "effect";

/** An opaque key. Provider-specific encoding and limits are checked by adapters. */
export const ObjectKey = Schema.NonEmptyString.check(
  Schema.makeFilter<string>((value) => value.isWellFormed() && !value.includes("\0"), {
    expected: "a well-formed opaque key without NUL",
  }),
);

export type ObjectKey = typeof ObjectKey.Type;

/** A JSON-safe byte count. Unsafe integers are rejected rather than rounded. */
export const ObjectSize = Schema.Natural;

export type ObjectSize = typeof ObjectSize.Type;

export const NativeVersion = Schema.Union([
  Schema.TaggedStruct("S3Version", { id: Schema.NonEmptyString }),
  Schema.TaggedStruct("GcsGeneration", {
    id: Schema.NonEmptyString,
    metageneration: Schema.optional(Schema.NonEmptyString),
  }),
  Schema.TaggedStruct("AzureVersion", { id: Schema.NonEmptyString }),
  Schema.TaggedStruct("AzureSnapshot", { id: Schema.NonEmptyString }),
  Schema.TaggedStruct("OpaqueVersion", {
    provider: Schema.NonEmptyString,
    id: Schema.NonEmptyString,
  }),
]);

export type NativeVersion = typeof NativeVersion.Type;

export const ObjectRef = Schema.Struct({
  storeId: Schema.NonEmptyString,
  key: ObjectKey,
  version: Schema.optional(NativeVersion),
});

export type ObjectRef = typeof ObjectRef.Type;

export const Checksum = Schema.Struct({
  algorithm: Schema.NonEmptyString,
  encoding: Schema.Literals(["hex", "base64"]),
  scope: Schema.Literals(["full-object", "composite"]),
  value: Schema.NonEmptyString,
  partCount: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
});

export type Checksum = typeof Checksum.Type;

export const HeaderValue = Schema.String.check(
  Schema.makeFilter<string>((value) => value.isWellFormed() && !/[\r\n\0]/u.test(value), {
    expected: "a well-formed header value without CR, LF or NUL",
  }),
);

export const ObjectValidator = HeaderValue.check(Schema.isNonEmpty());
export type ObjectValidator = typeof ObjectValidator.Type;

export const ObjectHeaders = Schema.Struct({
  contentType: Schema.optional(HeaderValue),
  contentEncoding: Schema.optional(HeaderValue),
  contentLanguage: Schema.optional(HeaderValue),
  contentDisposition: Schema.optional(HeaderValue),
  cacheControl: Schema.optional(HeaderValue),
  customMetadata: Schema.optional(
    Schema.Record(Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._-]+$/u)), HeaderValue),
  ),
});

export type ObjectHeaders = typeof ObjectHeaders.Type;

/** Type-side timestamps are DateTime.Utc; the encoded representation uses ISO strings. */
export const ObjectMetadata = Schema.Struct({
  ...ObjectHeaders.fields,
  ref: ObjectRef,
  size: Schema.optional(ObjectSize),
  etag: Schema.optional(ObjectValidator),
  lastModified: Schema.optional(Schema.DateTimeUtcFromString),
  checksums: Schema.optional(Schema.Array(Checksum)),
  storageClass: Schema.optional(Schema.NonEmptyString),
});

export type ObjectMetadata = typeof ObjectMetadata.Type;
