import { createHash } from "node:crypto";

import { Effect, Redacted, Schema } from "effect";

import type { S3Client, S3ClientConfig } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import { ObjectValidator } from "../object.js";
import type { S3Profile } from "./profile.js";

export type S3Encryption =
  | { readonly mode: "AES256" }
  | {
      readonly mode: "aws:kms" | "aws:kms:dsse";
      readonly keyId?: string;
      readonly bucketKey?: boolean;
    }
  | { readonly mode: "customer"; readonly key: Redacted.Redacted<string>; readonly keyMD5: string };

export interface S3Config {
  readonly bucket: string;
  readonly region: string;
  readonly storeId?: string;
  readonly endpoint?: string;
  readonly forcePathStyle?: boolean;
  readonly credentials?: S3ClientConfig["credentials"];
  readonly expectedBucketOwner?: string;
  readonly requestPayer?: "requester";
  readonly encryption?: S3Encryption;
  readonly requestTimeoutMillis?: number;
  /** Unknown-length streams are staged up to this explicit adapter limit. */
  readonly maxUnknownLengthBytes?: number;
}

const Positive = Schema.Int.check(Schema.isGreaterThan(0));
const Settings = Schema.Struct({
  bucket: Schema.String.check(
    Schema.isPattern(/^(?!.*\.\.)(?!.*--x-s3$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u),
  ),
  region: Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/u)),
  storeId: Schema.optional(Schema.NonEmptyString),
  endpoint: Schema.optional(Schema.String),
  forcePathStyle: Schema.optional(Schema.Boolean),
  credentials: Schema.optional(Schema.Unknown),
  expectedBucketOwner: Schema.optional(Schema.String.check(Schema.isPattern(/^\d{12}$/u))),
  requestPayer: Schema.optional(Schema.Literal("requester")),
  encryption: Schema.optional(
    Schema.Union([
      Schema.Struct({ mode: Schema.Literal("AES256") }),
      Schema.Struct({
        mode: Schema.Literals(["aws:kms", "aws:kms:dsse"]),
        keyId: Schema.optional(ObjectValidator),
        bucketKey: Schema.optional(Schema.Boolean),
      }),
      Schema.Struct({
        mode: Schema.Literal("customer"),
        key: Schema.Redacted(Schema.NonEmptyString),
        keyMD5: ObjectValidator,
      }),
    ]),
  ),
  requestTimeoutMillis: Schema.optional(Positive),
  maxUnknownLengthBytes: Schema.optional(Positive),
});

export type ResolvedS3Config = Omit<
  S3Config,
  "storeId" | "requestTimeoutMillis" | "maxUnknownLengthBytes"
> & {
  readonly storeId: string;
  readonly requestTimeoutMillis: number;
  readonly maxUnknownLengthBytes: number;
};

export interface S3Connection {
  readonly profile?: S3Profile;
  /** Compatibility transports may lack the optional S3 checksum read extension. */
  readonly readChecksums?: false;
  readonly multipartChecksums?: false;
  readonly client: S3Client;
  readonly config: ResolvedS3Config;
}

export const resolveConfig = Effect.fnUntraced(function* (config: S3Config) {
  yield* Schema.decodeUnknownEffect(Settings, { onExcessProperty: "error" })(config).pipe(
    Effect.mapError(() =>
      storageError("make", "s3", { _tag: "InvalidConfiguration", field: "config" }),
    ),
  );

  if (config.endpoint !== undefined) {
    const url = URL.parse(config.endpoint);
    if (
      !url ||
      !["https:", "http:"].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      url.pathname !== "/"
    ) {
      return yield* storageError("make", "s3", { _tag: "InvalidConfiguration", field: "endpoint" });
    }
  }

  if (config.encryption?.mode === "customer" && config.endpoint?.startsWith("http:"))
    return yield* storageError("make", "s3", {
      _tag: "InvalidConfiguration",
      field: "encryption.endpoint",
    });
  const identity = JSON.stringify([config.endpoint ?? "aws", config.region, config.bucket]);
  return {
    ...config,
    ...(config.encryption ? { encryption: { ...config.encryption } } : {}),
    storeId: config.storeId ?? `s3:${createHash("sha256").update(identity).digest("hex")}`,
    requestTimeoutMillis: config.requestTimeoutMillis ?? 30_000,
    maxUnknownLengthBytes: config.maxUnknownLengthBytes ?? 16 * 1024 * 1024,
  } satisfies ResolvedS3Config;
});

export function encryptionHeaders(encryption?: S3Encryption) {
  if (!encryption) return {};
  if (encryption.mode === "customer") {
    return {
      SSECustomerAlgorithm: "AES256",
      SSECustomerKey: Redacted.value(encryption.key),
      SSECustomerKeyMD5: encryption.keyMD5,
    };
  }
  return {
    ServerSideEncryption: encryption.mode,
    ...(encryption.mode === "AES256"
      ? {}
      : { SSEKMSKeyId: encryption.keyId, BucketKeyEnabled: encryption.bucketKey }),
  };
}

export function readEncryption(encryption?: S3Encryption) {
  return encryption?.mode === "customer" ? encryptionHeaders(encryption) : {};
}

export function authority(config: ResolvedS3Config) {
  return {
    Bucket: config.bucket,
    ExpectedBucketOwner: config.expectedBucketOwner,
    RequestPayer: config.requestPayer,
  };
}
