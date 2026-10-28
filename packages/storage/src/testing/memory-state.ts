import { Result, Schema } from "effect";
import type { Ref } from "effect";

import type { StorageCapabilities } from "../capabilities.js";
import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import type { ObjectMetadata } from "../object.js";
import type { ObjectTarget } from "../requests.js";

const Positive = Schema.Int.check(Schema.isGreaterThan(0));
export const MemoryOptions = Schema.Struct({
  storeId: Schema.optional(Schema.NonEmptyString),
  maxObjectBytes: Schema.optional(Positive),
  maxTotalBytes: Schema.optional(Positive),
  maxPendingBytes: Schema.optional(Positive),
  maxReadBytes: Schema.optional(Positive),
  maxObjects: Schema.optional(Positive),
  maxKeyBytes: Schema.optional(Positive),
  maxMetadataBytes: Schema.optional(Positive),
  chunkSize: Schema.optional(Positive),
});

export type MemoryOptions = typeof MemoryOptions.Type;
export type MemoryLimits = { readonly [Key in Exclude<keyof MemoryOptions, "storeId">]: number };

export interface StoredObject {
  readonly metadata: ObjectMetadata;
  readonly bytes: Uint8Array;
}

export interface MemoryState {
  readonly objects: ReadonlyMap<string, StoredObject>;
  readonly revision: bigint;
  readonly totalBytes: number;
  readonly pendingBytes: number;
  readonly activeReadBytes: number;
}

export interface MemoryContext {
  readonly storeId: string;
  readonly instanceId: string;
  readonly state: Ref.Ref<MemoryState>;
  readonly limits: MemoryLimits;
}

export const memoryCapabilities: StorageCapabilities = {
  putConditions: ["create-only", "if-match"],
  getConditions: ["if-match", "if-none-match"],
  deleteConditions: ["if-match"],
  ranges: ["bounded", "from", "suffix"],
  versionedReads: "current-only",
  list: { maxPageSize: 1000, delimiter: true, cursorPersistence: "process", snapshot: false },
};

export function matchesVersion(entry: StoredObject, target: ObjectTarget): boolean {
  if (!("ref" in target) || target.ref.version === undefined) return true;

  const version = target.ref.version;
  return (
    version._tag === "OpaqueVersion" &&
    version.provider === "memory" &&
    version.id === entry.metadata.ref.version?.id
  );
}

export function findObject(
  state: MemoryState,
  target: ObjectTarget,
  operation: string,
): Result.Result<StoredObject, StorageError> {
  const key = "ref" in target ? target.ref.key : target.key;
  const entry = state.objects.get(key);
  return entry && matchesVersion(entry, target)
    ? Result.succeed(entry)
    : Result.fail(storageError(operation, "memory", { _tag: "NotFound" }));
}

export function cloneMetadata(metadata: ObjectMetadata): ObjectMetadata {
  return {
    ...metadata,
    ref: {
      ...metadata.ref,
      ...(metadata.ref.version ? { version: { ...metadata.ref.version } } : {}),
    },
    ...(metadata.customMetadata ? { customMetadata: { ...metadata.customMetadata } } : {}),
    ...(metadata.checksums
      ? { checksums: metadata.checksums.map((checksum) => ({ ...checksum })) }
      : {}),
  };
}
