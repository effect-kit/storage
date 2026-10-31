import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { parseArgs, parseEnv } from "node:util";

import { Schema } from "effect";

export const providerIds = [
  "s3",
  "r2",
  "minio",
  "tigris",
  "spaces",
  "b2",
  "wasabi",
  "scaleway",
  "hetzner",
  "gcs",
  "azure",
  "supabase",
  "vercel",
  "r2-workers",
] as const;
export type ProviderId = (typeof providerIds)[number];
export const ProviderId = Schema.Literals(providerIds);
const Prefix = Schema.String.check(
  Schema.isPattern(/^effect-storage-acceptance\/[A-Za-z0-9_-]{1,80}\/$/u),
);
const JsonObject = Schema.Record(Schema.String, Schema.Unknown);
export interface ProviderSettings {
  readonly provider: ProviderId;
  readonly prefix: string;
  readonly config: Readonly<Record<string, unknown>>;
  readonly secrets: Readonly<Record<string, string>>;
}
export const variablePrefix = (provider: ProviderId) =>
  `STORAGE_TEST_${provider.toUpperCase().replaceAll("-", "_")}`;
export function options(args: ReadonlyArray<string>) {
  const { values } = parseArgs({
    args: args.filter((arg) => arg !== "--"),
    strict: true,
    options: {
      "credentials-file": { type: "string" },
      env: { type: "boolean" },
      provider: { type: "string" },
      suite: { type: "string", default: "portable" },
      "validate-only": { type: "boolean", default: false },
    },
  });
  if ((values["credentials-file"] === undefined) === (values.env !== true))
    throw new Error("Choose --credentials-file .env.providers.local or --env explicitly");
  if (!["portable", "signing"].includes(values.suite ?? "")) throw new Error("Unknown suite");
  return values;
}

export function configure(
  environment: Readonly<Record<string, string | undefined>>,
  selection?: string,
): ReadonlyArray<ProviderSettings> {
  const selected = selection ?? environment.STORAGE_TEST_PROVIDERS;
  if (!selected) throw new Error("Select providers with --provider or STORAGE_TEST_PROVIDERS");
  const ids =
    selected === "all"
      ? [...providerIds]
      : selected.split(",").map((id) => Schema.decodeUnknownSync(ProviderId)(id.trim()));
  if (ids.length === 0 || new Set(ids).size !== ids.length)
    throw new Error("Invalid provider selection");
  return ids.map((provider) => {
    const name = variablePrefix(provider);
    const required = (suffix: string) => {
      const key = `${name}_${suffix}`;
      const value = environment[key];
      if (!value) throw new Error(`Missing ${key}`);
      return value;
    };
    const prefix = Schema.decodeUnknownSync(Prefix)(required("PREFIX"));
    let config: Record<string, unknown>;
    try {
      config = Schema.decodeUnknownSync(JsonObject)(JSON.parse(required("CONFIG")));
    } catch {
      throw new Error(`Invalid ${name}_CONFIG JSON`);
    }
    const requiredFields =
      provider === "r2-workers"
        ? ["url"]
        : provider === "azure"
          ? ["containerUrl"]
          : provider === "vercel"
            ? ["storeId", "access"]
            : provider === "supabase"
              ? ["url", "bucket", "storeId"]
              : provider === "gcs"
                ? ["bucket"]
                : provider === "r2"
                  ? ["bucket", "accountId"]
                  : provider === "tigris"
                    ? ["bucket", "placement"]
                    : provider === "minio"
                      ? ["bucket", "region", "endpoint", "edition", "serverVersion"]
                      : provider === "spaces"
                        ? ["bucket", "region", "tier"]
                        : ["bucket", "region"];
    for (const field of requiredFields) {
      if (typeof config[field] !== "string" || config[field].length === 0)
        throw new Error(`Missing ${field} in ${name}_CONFIG`);
    }
    if (["minio", "hetzner"].includes(provider) && typeof config.versioning !== "boolean")
      throw new Error(`Missing versioning in ${name}_CONFIG`);
    const forbidden = [
      "credentials",
      "credential",
      "auth",
      "keyFilename",
      "customerKey",
      "encryption",
      "authClient",
      "leaseId",
    ];
    if (forbidden.some((key) => key in config))
      throw new Error(`Secret/authority overrides forbidden in ${name}_CONFIG`);
    for (const key of ["endpoint", "apiEndpoint", "containerUrl", "url"]) {
      if (config[key] === undefined) continue;
      const url = typeof config[key] === "string" ? URL.parse(config[key]) : null;
      const localMinio =
        provider === "minio" &&
        key === "endpoint" &&
        url?.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);

      if (
        !url ||
        (url.protocol !== "https:" && !localMinio) ||
        url.username ||
        url.password ||
        url.search ||
        url.hash
      )
        throw new Error(`Invalid HTTPS ${key} in ${name}_CONFIG`);
    }
    const suffixes =
      provider === "gcs"
        ? ["CREDENTIALS_JSON"]
        : provider === "azure"
          ? ["ACCOUNT_NAME", "ACCOUNT_KEY"]
          : ["supabase", "vercel", "r2-workers"].includes(provider)
            ? ["TOKEN"]
            : ["ACCESS_KEY_ID", "SECRET_ACCESS_KEY"];
    const secrets = Object.fromEntries(suffixes.map((suffix) => [suffix, required(suffix)]));
    if (provider === "gcs") {
      try {
        Schema.decodeUnknownSync(
          Schema.Struct({
            client_email: Schema.NonEmptyString,
            private_key: Schema.NonEmptyString,
          }),
        )(JSON.parse(secrets.CREDENTIALS_JSON ?? ""));
      } catch {
        throw new Error(`Invalid ${name}_CREDENTIALS_JSON`);
      }
    }
    if (
      provider === "supabase" &&
      !["user", "service"].includes(environment[`${name}_AUTH_MODE`] ?? "")
    )
      throw new Error(`Set ${name}_AUTH_MODE to user or service explicitly`);
    if (provider === "supabase") {
      secrets.AUTH_MODE = required("AUTH_MODE");
      if (environment[`${name}_API_KEY`]) secrets.API_KEY = required("API_KEY");
    }
    if (environment[`${name}_SESSION_TOKEN`]) secrets.SESSION_TOKEN = required("SESSION_TOKEN");
    return { provider, prefix, config, secrets };
  });
}
export async function loadSettings(args: ReturnType<typeof options>) {
  const environment =
    args["credentials-file"] === undefined
      ? process.env.STORAGE_PROVIDERS_ENV
        ? parseEnv(process.env.STORAGE_PROVIDERS_ENV)
        : process.env
      : parseEnv(await readFile(resolve(args["credentials-file"]), "utf8"));
  return configure(environment, args.provider);
}
