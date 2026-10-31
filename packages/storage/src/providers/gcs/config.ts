import { createHash } from "node:crypto";

import { Effect, Redacted, Schema } from "effect";

import type { Storage, StorageOptions } from "@google-cloud/storage";

import { storageError } from "../../core/errors.js";

export interface GcsConfig {
  readonly bucket: string;
  readonly projectId?: string;
  readonly storeId?: string;
  readonly apiEndpoint?: string;
  readonly credentials?: StorageOptions["credentials"];
  readonly authClient?: StorageOptions["authClient"];
  readonly keyFilename?: string;
  readonly useAuthWithCustomEndpoint?: boolean;
  readonly requestTimeoutMillis?: number;
  readonly maxUnknownLengthBytes?: number;
  readonly kmsKeyName?: string;
  readonly customerKey?: Redacted.Redacted<string>;
}
const Settings = Schema.Struct({
  bucket: Schema.NonEmptyString.check(Schema.isPattern(/^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$/u)),
  projectId: Schema.optional(Schema.NonEmptyString),
  storeId: Schema.optional(Schema.NonEmptyString),
  apiEndpoint: Schema.optional(Schema.String),
  credentials: Schema.optional(Schema.Unknown),
  authClient: Schema.optional(Schema.Unknown),
  keyFilename: Schema.optional(Schema.NonEmptyString),
  useAuthWithCustomEndpoint: Schema.optional(Schema.Boolean),
  requestTimeoutMillis: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
  maxUnknownLengthBytes: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
  kmsKeyName: Schema.optional(Schema.NonEmptyString),
  customerKey: Schema.optional(Schema.Redacted(Schema.NonEmptyString)),
});
export const resolveConfig = Effect.fnUntraced(function* (input: GcsConfig) {
  yield* Schema.decodeUnknownEffect(Settings, { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() =>
      storageError("make", "gcs", { _tag: "InvalidConfiguration", field: "config" }),
    ),
  );
  const endpoint = URL.parse(input.apiEndpoint ?? "https://storage.googleapis.com");
  if (
    !endpoint ||
    !["http:", "https:"].includes(endpoint.protocol) ||
    endpoint.username ||
    endpoint.password ||
    endpoint.search ||
    endpoint.hash ||
    endpoint.pathname !== "/" ||
    ((input.kmsKeyName || input.customerKey) && endpoint.protocol !== "https:")
  )
    return yield* storageError("make", "gcs", {
      _tag: "InvalidConfiguration",
      field: "apiEndpoint",
    });
  if (input.customerKey) {
    const key = Redacted.value(input.customerKey);
    if (
      input.kmsKeyName ||
      !/^[A-Za-z0-9+/]{43}=$/u.test(key) ||
      Buffer.from(key, "base64").length !== 32 ||
      Buffer.from(key, "base64").toString("base64") !== key
    )
      return yield* storageError("make", "gcs", {
        _tag: "InvalidConfiguration",
        field: "customerKey",
      });
  }
  return {
    bucket: input.bucket,
    projectId: input.projectId,
    storeId:
      input.storeId ??
      `gcs:${createHash("sha256")
        .update(JSON.stringify([endpoint.origin, input.bucket]))
        .digest("hex")}`,
    apiEndpoint: endpoint.origin,
    requestTimeoutMillis: input.requestTimeoutMillis ?? 30_000,
    maxUnknownLengthBytes: input.maxUnknownLengthBytes ?? 16 * 1024 * 1024,
    kmsKeyName: input.kmsKeyName,
    customerKey: input.customerKey,
  };
});
export type ResolvedGcsConfig = Effect.Success<ReturnType<typeof resolveConfig>>;
export interface GcsConnection {
  readonly client: Storage;
  readonly config: ResolvedGcsConfig;
}

export function customerEncryption(
  config: ResolvedGcsConfig,
  source = false,
): Record<string, string> {
  if (!config.customerKey) return {};
  const key = Redacted.value(config.customerKey);
  const prefix = source ? "x-goog-copy-source-encryption" : "x-goog-encryption";
  return {
    [`${prefix}-algorithm`]: "AES256",
    [`${prefix}-key`]: key,
    [`${prefix}-key-sha256`]: createHash("sha256")
      .update(Buffer.from(key, "base64"))
      .digest("base64"),
  };
}
