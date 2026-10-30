import { createHash } from "node:crypto";

import { Effect, Redacted, Schema } from "effect";

import type { ContainerClient, newPipeline } from "@azure/storage-blob";

import { storageError } from "../errors.js";
import { HeaderValue } from "../object.js";

export interface AzureConfig {
  /** Complete container URL; an existing SAS must be wrapped in Redacted. */
  readonly containerUrl: string | Redacted.Redacted<string>;
  readonly storeId?: string;
  readonly credential?: Parameters<typeof newPipeline>[0];
  readonly requestTimeoutMillis?: number;
  readonly maxUnknownLengthBytes?: number;
  readonly leaseId?: Redacted.Redacted<string>;
  readonly encryption?:
    | { readonly scope: string }
    | { readonly key: Redacted.Redacted<string>; readonly keySHA256: string };
  readonly checksumAlgorithm?: "None" | "StorageCrc64";
  readonly profile?: "block-blob";
}
export const resolveConfig = Effect.fnUntraced(function* (input: AzureConfig) {
  yield* Schema.decodeUnknownEffect(
    Schema.Struct({
      containerUrl: Schema.Union([Schema.String, Schema.Redacted(Schema.String)]),
      storeId: Schema.optional(Schema.NonEmptyString),
      credential: Schema.optional(Schema.Unknown),
      requestTimeoutMillis: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
      maxUnknownLengthBytes: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
      leaseId: Schema.optional(Schema.Redacted(Schema.NonEmptyString)),
      encryption: Schema.optional(
        Schema.Union([
          Schema.Struct({ scope: HeaderValue.check(Schema.isNonEmpty()) }),
          Schema.Struct({
            key: Schema.Redacted(Schema.NonEmptyString),
            keySHA256: Schema.NonEmptyString,
          }),
        ]),
      ),
      checksumAlgorithm: Schema.optional(Schema.Literals(["None", "StorageCrc64"])),
      profile: Schema.optional(Schema.Literal("block-blob")),
    }),
    { onExcessProperty: "error" },
  )(input).pipe(
    Effect.mapError(() =>
      storageError("make", "azure", { _tag: "InvalidConfiguration", field: "config" }),
    ),
  );
  const rawUrl = Redacted.isRedacted(input.containerUrl)
    ? Redacted.value(input.containerUrl)
    : input.containerUrl;
  const url = URL.parse(rawUrl);
  if (
    !url ||
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.hash ||
    url.pathname === "/" ||
    (url.search && !Redacted.isRedacted(input.containerUrl)) ||
    (input.encryption && url.protocol !== "https:")
  )
    return yield* storageError("make", "azure", {
      _tag: "InvalidConfiguration",
      field: "containerUrl-or-encryption",
    });
  if (input.encryption && "key" in input.encryption) {
    const encoded = Redacted.value(input.encryption.key);
    const bytes = Buffer.from(encoded, "base64");
    if (
      bytes.length !== 32 ||
      bytes.toString("base64") !== encoded ||
      createHash("sha256").update(bytes).digest("base64") !== input.encryption.keySHA256
    )
      return yield* storageError("make", "azure", {
        _tag: "InvalidConfiguration",
        field: "encryption.key",
      });
  }
  const publicUrl = `${url.origin}${url.pathname.replace(/\/$/u, "")}`;
  return {
    containerUrl: publicUrl,
    storeId: input.storeId ?? `azure:${createHash("sha256").update(publicUrl).digest("hex")}`,
    requestTimeoutMillis: input.requestTimeoutMillis ?? 30_000,
    maxUnknownLengthBytes: input.maxUnknownLengthBytes ?? 16 * 1024 * 1024,
    checksumAlgorithm: input.checksumAlgorithm ?? "None",
    leaseId: input.leaseId,
    encryption: input.encryption ? { ...input.encryption } : undefined,
  };
});
export type ResolvedAzureConfig = Effect.Success<ReturnType<typeof resolveConfig>>;
export interface AzureConnection {
  readonly client: ContainerClient;
  readonly config: ResolvedAzureConfig;
}
export function encryption(config: ResolvedAzureConfig) {
  return !config.encryption
    ? {}
    : "scope" in config.encryption
      ? { encryptionScope: config.encryption.scope }
      : {
          customerProvidedKey: {
            encryptionAlgorithm: "AES256" as const,
            encryptionKey: Redacted.value(config.encryption.key),
            encryptionKeySha256: config.encryption.keySHA256,
          },
        };
}
export function lease(config: ResolvedAzureConfig) {
  return config.leaseId ? { leaseId: Redacted.value(config.leaseId) } : {};
}
