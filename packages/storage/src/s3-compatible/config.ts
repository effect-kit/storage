import { createHash } from "node:crypto";

import { Effect, Schema } from "effect";

import { storageError } from "../errors.js";
import type { S3Config } from "../s3/config.js";
import { resolveConfig } from "../s3/config.js";
import { nativeLimits } from "../s3/native-models.js";
import type { S3Profile } from "../s3/profile.js";

export type CompatibleProvider =
  | "r2"
  | "minio"
  | "tigris"
  | "spaces"
  | "b2"
  | "wasabi"
  | "scaleway"
  | "hetzner";

export interface CompatibleConfig extends Pick<
  S3Config,
  | "bucket"
  | "storeId"
  | "credentials"
  | "encryption"
  | "requestTimeoutMillis"
  | "maxUnknownLengthBytes"
> {
  /** Explicit credentials prevent accidental AWS ambient credential discovery. */
  readonly credentials: NonNullable<S3Config["credentials"]>;
  /** Trusted gateway/region override; never derive this from an untrusted request. */
  readonly endpoint?: string;
  readonly forcePathStyle?: boolean;
}
export interface R2Config extends CompatibleConfig {
  readonly accountId: string;
  readonly jurisdiction?: "default" | "eu" | "fedramp";
}
export interface MinioConfig extends CompatibleConfig {
  readonly endpoint: string;
  readonly region: string;
  readonly edition: "community" | "aistor";
  readonly serverVersion: string;
  readonly versioning: boolean;
}
export interface TigrisConfig extends CompatibleConfig {
  readonly placement: "single-region" | "multi-region" | "global" | "dual-region";
}
export interface SpacesConfig extends CompatibleConfig {
  readonly region: string;
  readonly tier: "standard" | "cold";
}
export interface RegionalConfig extends CompatibleConfig {
  readonly region: string;
}
export interface HetznerConfig extends RegionalConfig {
  readonly versioning: boolean;
}

type Settings =
  | R2Config
  | MinioConfig
  | TigrisConfig
  | SpacesConfig
  | RegionalConfig
  | HetznerConfig;
const Region = Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/u));
const common = {
  bucket: Schema.String,
  storeId: Schema.optional(Schema.NonEmptyString),
  credentials: Schema.Unknown,
  encryption: Schema.optional(Schema.Unknown),
  endpoint: Schema.optional(Schema.String),
  forcePathStyle: Schema.optional(Schema.Boolean),
  requestTimeoutMillis: Schema.optional(Schema.Number),
  maxUnknownLengthBytes: Schema.optional(Schema.Number),
};
const profiles = {
  r2: Schema.Struct({
    ...common,
    accountId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/u)),
    jurisdiction: Schema.optional(Schema.Literals(["default", "eu", "fedramp"])),
  }),
  minio: Schema.Struct({
    ...common,
    endpoint: Schema.String,
    region: Region,
    edition: Schema.Literals(["community", "aistor"]),
    serverVersion: Schema.NonEmptyString,
    versioning: Schema.Boolean,
  }),
  tigris: Schema.Struct({
    ...common,
    placement: Schema.Literals(["single-region", "multi-region", "global", "dual-region"]),
  }),
  spaces: Schema.Struct({ ...common, region: Region, tier: Schema.Literals(["standard", "cold"]) }),
  b2: Schema.Struct({ ...common, region: Region }),
  wasabi: Schema.Struct({ ...common, region: Region }),
  scaleway: Schema.Struct({ ...common, region: Region }),
  hetzner: Schema.Struct({ ...common, region: Region, versioning: Schema.Boolean }),
};

export const resolveCompatible = Effect.fnUntraced(function* (
  provider: CompatibleProvider,
  input: Settings,
) {
  yield* Schema.decodeUnknownEffect(profiles[provider], { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() =>
      storageError("make", provider, { _tag: "InvalidConfiguration", field: "config" }),
    ),
  );
  yield* Schema.decodeUnknownEffect(
    Schema.Union([
      Schema.declare<() => unknown>((value): value is () => unknown => typeof value === "function"),
      Schema.Struct({ accessKeyId: Schema.NonEmptyString, secretAccessKey: Schema.NonEmptyString }),
    ]),
  )(input.credentials).pipe(
    Effect.mapError(() =>
      storageError("make", provider, { _tag: "InvalidConfiguration", field: "credentials" }),
    ),
  );
  const region = "region" in input ? input.region : "auto";
  const jurisdiction = "jurisdiction" in input ? (input.jurisdiction ?? "default") : "default";
  const endpoint =
    input.endpoint ??
    (provider === "r2" && "accountId" in input
      ? `https://${input.accountId}${jurisdiction === "default" ? "" : `.${jurisdiction}`}.r2.cloudflarestorage.com`
      : provider === "tigris"
        ? "https://t3.storage.dev"
        : provider === "spaces"
          ? `https://${region}.digitaloceanspaces.com`
          : provider === "b2"
            ? `https://s3.${region}.backblazeb2.com`
            : provider === "wasabi"
              ? `https://s3.${region}.wasabisys.com`
              : provider === "scaleway"
                ? `https://s3.${region}.scw.cloud`
                : `https://${region}.your-objectstorage.com`);
  const url = URL.parse(endpoint);
  if (
    !url ||
    (url.protocol !== "https:" &&
      !(url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))
  )
    return yield* storageError("make", provider, {
      _tag: "InvalidConfiguration",
      field: "endpoint.tls",
    });
  const configuration = JSON.stringify([
    provider,
    region,
    endpoint,
    "versioning" in input ? input.versioning : null,
    "placement" in input ? input.placement : null,
    "tier" in input ? input.tier : null,
    "edition" in input ? [input.edition, input.serverVersion] : null,
  ]);
  const strongPlacement =
    provider === "tigris" &&
    "placement" in input &&
    ["single-region", "multi-region"].includes(input.placement);
  const putConditions: S3Profile["capabilities"]["putConditions"] =
    provider === "r2" || provider === "scaleway" || strongPlacement
      ? ["create-only", "if-match"]
      : [];
  const checksums = provider === "scaleway" || provider === "wasabi";
  const versioned =
    provider === "tigris" ||
    provider === "b2" ||
    provider === "wasabi" ||
    provider === "scaleway" ||
    ((provider === "minio" || provider === "hetzner") && "versioning" in input && input.versioning);
  const profile: S3Profile = Object.freeze<S3Profile>({
    provider,
    ...(provider === "spaces" && "tier" in input ? { bucketTier: input.tier } : {}),
    configuration,
    capabilities: {
      putConditions,
      getConditions: ["if-match", "if-none-match"],
      deleteConditions: [],
      ranges: ["bounded", "from", "suffix"],
      versionedReads: versioned ? "history" : "none",
      list: { maxPageSize: 1000, delimiter: true, cursorPersistence: "provider", snapshot: false },
    },
    // Conservative byte ceilings: provider GB/TB spellings are interpreted as decimal.
    // Scaleway's 1,000-part constraint is intentionally independent of AWS's 10,000.
    limits: {
      ...nativeLimits,
      maximumPartBytes: 5_000_000_000,
      maximumSinglePutBytes: 5_000_000_000,
      maximumSingleCopyBytes: 5_000_000_000,
      maximumParts: provider === "scaleway" ? 1000 : 10000,
    },
    maximumObjectBytes: 5_000_000_000_000,
    checksums,
    completeConditions: provider === "scaleway" ? ["create-only", "if-match"] : [],
    copy:
      provider === "spaces" && "tier" in input && input.tier === "cold"
        ? "none"
        : provider === "hetzner"
          ? "bucket"
          : "endpoint",
    copyPartConditions: !["r2", "hetzner"].includes(provider),
    post:
      ["minio", "spaces", "wasabi"].includes(provider) &&
      !("tier" in input && input.tier === "cold"),
    storageClasses: provider === "r2" ? ["STANDARD", "STANDARD_IA"] : ["STANDARD"],
    tagging: ["minio", "wasabi", "scaleway"].includes(provider),
  });
  const config = yield* resolveConfig({
    bucket: input.bucket,
    region: provider === "spaces" ? "us-east-1" : region,
    endpoint,
    forcePathStyle: input.forcePathStyle ?? provider === "minio",
    storeId:
      input.storeId ??
      `${provider}:${createHash("sha256")
        .update(JSON.stringify([configuration, input.bucket]))
        .digest("hex")}`,
    credentials: input.credentials,
    ...(input.encryption ? { encryption: input.encryption } : {}),
    ...(input.requestTimeoutMillis !== undefined
      ? { requestTimeoutMillis: input.requestTimeoutMillis }
      : {}),
    ...(input.maxUnknownLengthBytes !== undefined
      ? { maxUnknownLengthBytes: input.maxUnknownLengthBytes }
      : {}),
  }).pipe(
    Effect.mapError((error) =>
      storageError(error.operation, provider, error.reason, error.mutationOutcome),
    ),
  );
  const encryption = input.encryption;
  if (
    encryption &&
    (["tigris", "spaces"].includes(provider) ||
      (["r2", "wasabi"].includes(provider) && encryption.mode !== "customer") ||
      (provider !== "minio" && provider !== "wasabi" && encryption.mode.startsWith("aws:")))
  )
    return yield* storageError(
      "make",
      provider,
      { _tag: "Unsupported", capability: "encryption-mode" },
      "not-started",
    );
  return { profile, config };
});
