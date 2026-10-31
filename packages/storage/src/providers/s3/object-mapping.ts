import { DateTime, Effect, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import { ObjectRef } from "../../core/object.js";
import type { Checksum, ObjectHeaders, ObjectMetadata } from "../../core/object.js";
import type { ObjectTarget, WriteCondition } from "../../core/requests.js";
import { decodeRequest } from "../../internal/request-validation.js";
import type { ResolvedS3Config, S3Connection } from "./config.js";
import { S3Key } from "./validation.js";

const checksumFields = {
  ChecksumCRC32: "crc32",
  ChecksumCRC32C: "crc32c",
  ChecksumCRC64NVME: "crc64nvme",
  ChecksumSHA1: "sha1",
  ChecksumSHA256: "sha256",
  ChecksumSHA512: "sha512",
  ChecksumMD5: "md5",
  ChecksumXXHASH64: "xxhash64",
  ChecksumXXHASH3: "xxhash3",
  ChecksumXXHASH128: "xxhash128",
} as const;

export interface NativeMetadata {
  readonly ContentLength?: number | undefined;
  readonly ContentType?: string | undefined;
  readonly ContentEncoding?: string | undefined;
  readonly ContentLanguage?: string | undefined;
  readonly ContentDisposition?: string | undefined;
  readonly CacheControl?: string | undefined;
  readonly Metadata?: Record<string, string> | undefined;
  readonly VersionId?: string | undefined;
  readonly ETag?: string | undefined;
  readonly LastModified?: Date | undefined;
  readonly StorageClass?: string | undefined;
  readonly ChecksumType?: string | undefined;
}

export function checksums(output: NativeMetadata): ReadonlyArray<Checksum> | undefined {
  const found: Checksum[] = [];
  for (const [field, algorithm] of Object.entries(checksumFields)) {
    const value: unknown = Reflect.get(output, field);
    if (typeof value === "string") {
      const suffix = /-(\d+)$/u.exec(value)?.[1];
      const count = suffix === undefined ? undefined : Number(suffix);
      found.push({
        algorithm,
        value,
        encoding: "base64",
        scope:
          output.ChecksumType === "COMPOSITE" || suffix !== undefined ? "composite" : "full-object",
        ...(count !== undefined && Number.isSafeInteger(count) && count > 0
          ? { partCount: count }
          : {}),
      });
    }
  }
  return found.length ? found : undefined;
}

export function metadata(
  connection: Pick<S3Connection, "config" | "profile">,
  key: string,
  output: NativeMetadata,
): ObjectMetadata {
  // Some compatible services return upload identities that cannot be used for versioned reads.
  const versioned = connection.profile?.capabilities.versionedReads !== "none";

  return {
    ref: {
      storeId: connection.config.storeId,
      key,
      ...(!versioned || output.VersionId === undefined
        ? {}
        : { version: { _tag: "S3Version", id: output.VersionId } }),
    },
    size: output.ContentLength,
    etag: output.ETag,
    lastModified:
      output.LastModified === undefined ? undefined : DateTime.makeUnsafe(output.LastModified),
    contentType: output.ContentType,
    contentEncoding: output.ContentEncoding,
    contentLanguage: output.ContentLanguage,
    contentDisposition: output.ContentDisposition,
    cacheControl: output.CacheControl,
    customMetadata: output.Metadata,
    storageClass: output.StorageClass,
    checksums: checksums(output),
  };
}

export function headers(input: ObjectHeaders) {
  return {
    ContentType: input.contentType,
    ContentEncoding: input.contentEncoding,
    ContentLanguage: input.contentLanguage,
    ContentDisposition: input.contentDisposition,
    CacheControl: input.cacheControl,
    Metadata: input.customMetadata ? { ...input.customMetadata } : undefined,
  };
}

export function condition(input?: WriteCondition) {
  return input?._tag === "CreateOnly" ? { IfNoneMatch: "*" } : { IfMatch: input?.etag };
}

export function target(input: ObjectTarget) {
  return "ref" in input
    ? { Key: input.ref.key, VersionId: input.ref.version?.id }
    : { Key: input.key };
}

const mutationOperations: ReadonlySet<string> = new Set([
  "put",
  "delete",
  "deleteObjects",
  "copy",
  "copyPart",
  "initiate",
  "uploadPart",
  "complete",
  "abort",
]);

export const validateRef = (ref: ObjectRef, config: ResolvedS3Config, operation: string) =>
  decodeRequest(
    Schema.Struct({ ...ObjectRef.fields, key: S3Key }),
    ref,
    operation,
    "s3",
    mutationOperations.has(operation),
  ).pipe(
    Effect.flatMap((input) => {
      const error = refError(input, config, operation);
      return error ? Effect.fail(error) : Effect.succeed(input);
    }),
  );

export function refError(ref: ObjectRef, config: ResolvedS3Config, operation: string) {
  return ref.storeId !== config.storeId
    ? storageError(
        operation,
        "s3",
        { _tag: "InvalidInput", field: "ref.storeId" },
        mutationOperations.has(operation) ? "not-started" : "not-applicable",
      )
    : ref.version && ref.version._tag !== "S3Version"
      ? storageError(
          operation,
          "s3",
          { _tag: "Unsupported", capability: "native-version-kind" },
          mutationOperations.has(operation) ? "not-started" : "not-applicable",
        )
      : undefined;
}

export function encodeCopySource(bucket: string, ref: ObjectRef): string {
  const path = `${encodeURIComponent(bucket)}/${ref.key.split("/").map(encodeURIComponent).join("/")}`;
  return ref.version ? `${path}?versionId=${encodeURIComponent(ref.version.id)}` : path;
}
