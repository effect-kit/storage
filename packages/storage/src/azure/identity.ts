import { DefaultAzureCredential } from "@azure/identity";
import type { DefaultAzureCredentialOptions } from "@azure/identity";

/** Optional Azure identity peer is loaded only by this entry point. */
export const defaultCredential = (options?: DefaultAzureCredentialOptions) =>
  new DefaultAzureCredential(options);
