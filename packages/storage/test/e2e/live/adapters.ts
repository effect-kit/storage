import { Layer, Redacted } from "effect";

import type { ProviderSettings } from "../../../../../scripts/providers/configuration.ts";
import type { ProviderLayer } from "./suite.ts";

const names = {
  s3: "S3Storage",
  r2: "R2Storage",
  minio: "MinioStorage",
  tigris: "TigrisStorage",
  spaces: "SpacesStorage",
  b2: "B2Storage",
  wasabi: "WasabiStorage",
  scaleway: "ScalewayStorage",
  hetzner: "HetznerStorage",
  gcs: "GcsStorage",
  azure: "AzureStorage",
  supabase: "SupabaseStorage",
  vercel: "VercelStorage",
};

export async function providerLayer(
  settings: ProviderSettings,
  suite: string,
): Promise<ProviderLayer> {
  const { provider, config, secrets } = settings;
  if (provider === "r2-workers") throw new Error("Worker requires its remote runtime");

  const secret = (name: string) => {
    const value = secrets[name];
    if (!value) throw new Error("Missing explicit provider credential");
    return value;
  };
  const adapter = await import(`@effect-kit/storage/${provider}`);
  let options;
  let signingAuthority;
  if (provider === "gcs") {
    options = { ...config, credentials: JSON.parse(secret("CREDENTIALS_JSON")) };
  } else if (provider === "azure") {
    const { StorageSharedKeyCredential } = await import("@azure/storage-blob");
    const credential = new StorageSharedKeyCredential(
      secret("ACCOUNT_NAME"),
      secret("ACCOUNT_KEY"),
    );
    options = { ...config, credential };
    signingAuthority = { kind: "shared-key", credential };
  } else if (provider === "supabase") {
    options = {
      ...config,
      auth: {
        mode: secret("AUTH_MODE"),
        token: Redacted.make(secret("TOKEN")),
        ...(secrets.API_KEY ? { apiKey: Redacted.make(secrets.API_KEY) } : {}),
      },
    };
  } else if (provider === "vercel") {
    options = { ...config, credential: { mode: "token", token: Redacted.make(secret("TOKEN")) } };
  } else {
    options = {
      ...config,
      credentials: {
        accessKeyId: secret("ACCESS_KEY_ID"),
        secretAccessKey: secret("SECRET_ACCESS_KEY"),
        ...(secrets.SESSION_TOKEN ? { sessionToken: secrets.SESSION_TOKEN } : {}),
      },
    };
  }

  const layer: ProviderLayer = adapter[names[provider]].layer({
    ...options,
    requestTimeoutMillis: 15000,
    maxUnknownLengthBytes: 1024 * 1024,
  });
  if (!["signing", "release", "browser"].includes(suite)) return layer;

  const signing =
    provider === "gcs" || provider === "azure" || provider === "supabase" || provider === "vercel"
      ? provider
      : "s3";
  const module = await import(`@effect-kit/storage/${signing}/signing`);
  const tag = {
    gcs: "GcsSigning",
    azure: "AzureSigning",
    supabase: "SupabaseSigning",
    vercel: "VercelSigning",
    s3: "S3Signing",
  }[signing];
  const signer = signing === "s3" ? module[tag].layer : module[tag].layer(signingAuthority);
  return signer.pipe(Layer.provideMerge(layer));
}
