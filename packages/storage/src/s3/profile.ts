import { Effect } from "effect";

import type { StorageCapabilities } from "../capabilities.js";
import { storageError } from "../errors.js";
import type { ObjectRef } from "../object.js";
import type { S3Connection } from "./config.js";
import type { S3ChecksumOptions } from "./multipart.js";
import type { nativeLimits } from "./native-models.js";

export interface S3Profile {
  readonly provider: string;
  readonly configuration: string;
  readonly capabilities: StorageCapabilities;
  readonly limits: Omit<typeof nativeLimits, "maximumParts"> & { readonly maximumParts: number };
  readonly maximumObjectBytes: number;
  readonly checksums: boolean;
  readonly completeConditions: ReadonlyArray<"create-only" | "if-match">;
  readonly copy: "endpoint" | "bucket" | "none";
  readonly copyPartConditions: boolean;
  readonly post: boolean;
  readonly storageClasses: readonly string[];
  readonly tagging: boolean;
}

interface ProfileRequest {
  readonly key?: string | undefined;
  readonly ref?: ObjectRef | undefined;
  readonly source?: ObjectRef | undefined;
  readonly upload?:
    | {
        readonly ref: ObjectRef;
        readonly checksumAlgorithm?: string | undefined;
        readonly checksumType?: string | undefined;
      }
    | undefined;
  readonly condition?: { readonly _tag: string } | undefined;
  readonly method?: string | undefined;
  readonly checksum?: S3ChecksumOptions | undefined;
  readonly partNumber?: number | undefined;
  readonly contentLength?: number | undefined;
  readonly parts?:
    | ReadonlyArray<{
        readonly size?: number | undefined;
        readonly partNumber: number;
        readonly checksums?:
          | ReadonlyArray<{ readonly algorithm: string; readonly scope: string }>
          | undefined;
      }>
    | undefined;
}

/** Applied before source acquisition or signing, including native escape-hatch methods. */
export const enforceProfile = Effect.fnUntraced(function* (
  connection: S3Connection,
  operation: string,
  input: ProfileRequest,
) {
  const p = connection.profile;
  if (!p) return;
  const reject = (capability: string) =>
    storageError(operation, p.provider, { _tag: "Unsupported", capability }, "not-started");
  if (
    p.capabilities.versionedReads === "none" &&
    (input.ref?.version || input.source?.version || input.upload?.ref.version)
  )
    return yield* reject("native-versions");
  const mutation = operation === "sign" ? input.method : operation;
  const conditions =
    mutation === "put" || mutation === "PUT"
      ? p.capabilities.putConditions
      : mutation === "complete"
        ? p.completeConditions
        : [];
  if (input.condition && !["get", "head"].includes(operation)) {
    const kind = input.condition._tag === "CreateOnly" ? "create-only" : "if-match";
    if (!conditions.includes(kind)) return yield* reject(`${operation}-condition`);
  }
  if (input.partNumber !== undefined && input.partNumber > p.limits.maximumParts)
    return yield* reject("maximum-parts");
  if (
    input.parts &&
    (input.parts.length > p.limits.maximumParts ||
      input.parts.some(
        (part) =>
          part.partNumber > p.limits.maximumParts ||
          (part.size !== undefined && part.size > p.limits.maximumPartBytes),
      ))
  )
    return yield* reject("part-limits");
  if (
    input.parts &&
    input.parts.reduce((sum, part) => sum + (part.size ?? 0), 0) > p.maximumObjectBytes
  )
    return yield* reject("maximum-object-bytes");
  const maximum =
    operation === "uploadPart" || operation === "signPart"
      ? p.limits.maximumPartBytes
      : p.limits.maximumSinglePutBytes;
  if (input.contentLength !== undefined && input.contentLength > maximum)
    return yield* reject("maximum-request-bytes");
  if (
    !p.checksums &&
    (Object.entries(input.checksum ?? {}).some(
      ([key, value]) => key.startsWith("Checksum") && value !== undefined,
    ) ||
      input.upload?.checksumAlgorithm ||
      input.upload?.checksumType ||
      input.parts?.some((part) => part.checksums?.length))
  )
    return yield* reject("unqualified-checksum-combination");
  if (p.checksums) {
    const algorithm = input.checksum?.ChecksumAlgorithm ?? input.upload?.checksumAlgorithm;
    const type = input.checksum?.ChecksumType ?? input.upload?.checksumType;
    const allowed = ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"];
    if (
      (algorithm && !allowed.includes(algorithm)) ||
      Object.keys(input.checksum ?? {}).some(
        (key) =>
          key.startsWith("Checksum") &&
          !["ChecksumAlgorithm", "ChecksumType", ...allowed.map((a) => `Checksum${a}`)].includes(
            key,
          ),
      )
    )
      return yield* reject("checksum-algorithm");
    if (
      (algorithm === "CRC64NVME" && type === "COMPOSITE") ||
      (["SHA1", "SHA256"].includes(algorithm ?? "") && type === "FULL_OBJECT")
    )
      return yield* reject("checksum-type");
  }
});
