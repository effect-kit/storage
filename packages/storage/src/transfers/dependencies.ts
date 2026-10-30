import { Context } from "effect";

import type { TransferDigest, TransferEndpoint } from "./models.js";

export const endpointStoreIds = (endpoint: TransferEndpoint): ReadonlyArray<string> =>
  endpoint.underlyingStoreIds ?? [endpoint.underlyingStoreId ?? endpoint.storage.storeId];

/** Provider protocol capabilities for one configured Storage instance. */
export class TransferDriver extends Context.Service<TransferDriver, TransferEndpoint>()(
  "@effect-kit/storage/TransferDriver",
) {}

/** Incremental digest implementation supplied at the application's runtime boundary. */
export class TransferHash extends Context.Service<TransferHash, TransferDigest>()(
  "@effect-kit/storage/TransferHash",
) {}
