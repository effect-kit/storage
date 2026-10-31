import { Schema, Struct } from "effect";

import { ObjectHeaders, ObjectRef, ObjectSize, ObjectValidator } from "../../core/object.js";
import { WriteCondition } from "../../core/requests.js";
import { S3Key } from "./validation.js";

export const ChecksumOptions = Schema.Struct({
  ChecksumAlgorithm: Schema.optional(
    Schema.Literals([
      "CRC32",
      "CRC32C",
      "CRC64NVME",
      "SHA1",
      "SHA256",
      "SHA512",
      "MD5",
      "XXHASH64",
      "XXHASH3",
      "XXHASH128",
    ]),
  ),
  ChecksumCRC32: Schema.optional(ObjectValidator),
  ChecksumCRC32C: Schema.optional(ObjectValidator),
  ChecksumCRC64NVME: Schema.optional(ObjectValidator),
  ChecksumSHA1: Schema.optional(ObjectValidator),
  ChecksumSHA256: Schema.optional(ObjectValidator),
  ChecksumSHA512: Schema.optional(ObjectValidator),
  ChecksumMD5: Schema.optional(ObjectValidator),
  ChecksumXXHASH64: Schema.optional(ObjectValidator),
  ChecksumXXHASH3: Schema.optional(ObjectValidator),
  ChecksumXXHASH128: Schema.optional(ObjectValidator),
  ChecksumType: Schema.optional(Schema.Literals(["FULL_OBJECT", "COMPOSITE"])),
});

export const PutOptions = Schema.Struct({
  ...Struct.omit(ChecksumOptions.fields, ["ChecksumType"]),
  StorageClass: Schema.optional(
    Schema.Literals([
      "STANDARD",
      "REDUCED_REDUNDANCY",
      "STANDARD_IA",
      "ONEZONE_IA",
      "INTELLIGENT_TIERING",
      "GLACIER",
      "DEEP_ARCHIVE",
      "GLACIER_IR",
      "EXPRESS_ONEZONE",
      "OUTPOSTS",
      "SNOW",
    ]),
  ),
  Tagging: Schema.optional(ObjectValidator),
});

export const Upload = Schema.Struct({
  ref: ObjectRef,
  uploadId: Schema.Redacted(Schema.NonEmptyString),
  checksumAlgorithm: ChecksumOptions.fields.ChecksumAlgorithm,
  checksumType: ChecksumOptions.fields.ChecksumType,
});

export const PartNumber = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10000 }));
export const CompletePart = Schema.Struct({
  partNumber: PartNumber,
  etag: ObjectValidator,
  size: Schema.optional(ObjectSize),
  checksums: Schema.optional(
    Schema.Array(
      Schema.Struct({
        algorithm: Schema.String,
        encoding: Schema.Literals(["hex", "base64"]),
        scope: Schema.Literals(["full-object", "composite"]),
        value: Schema.String,
        partCount: Schema.optional(Schema.Natural),
      }),
    ),
  ),
});

export const CopyRequest = Schema.Struct({
  source: ObjectRef,
  destinationKey: S3Key,
  condition: Schema.optional(WriteCondition),
  sourceIfMatch: Schema.optional(ObjectValidator),
  headers: Schema.optional(ObjectHeaders),
  metadataDirective: Schema.optional(Schema.Literals(["COPY", "REPLACE"])),
  taggingDirective: Schema.optional(Schema.Literals(["COPY", "REPLACE"])),
  tagging: Schema.optional(ObjectValidator),
});
export type S3CopyRequest = typeof CopyRequest.Type;

export const nativeLimits = Object.freeze({
  minimumPartBytes: 5 * 1024 * 1024,
  maximumPartBytes: 5 * 1024 * 1024 * 1024,
  maximumParts: 10000,
  maximumSinglePutBytes: 5 * 1024 * 1024 * 1024,
  maximumSingleCopyBytes: 5 * 1024 * 1024 * 1024,
  maximumPageSize: 1000,
});
