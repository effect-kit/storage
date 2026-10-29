import { Context } from "effect";

import type { TransferDigest, TransferEndpoint } from "./models.js";

/** Provider protocol capabilities for one configured Storage instance. */
export class TransferDriver extends Context.Service<TransferDriver, TransferEndpoint>()(
  "@effect-kit/storage/TransferDriver",
) {}

/** Incremental digest implementation supplied at the application's runtime boundary. */
export class TransferHash extends Context.Service<TransferHash, TransferDigest>()(
  "@effect-kit/storage/TransferHash",
) {}
