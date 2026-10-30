import { Context, Effect, Layer } from "effect";

import { Storage } from "../storage.js";
import { TransferDriver } from "../transfers/dependencies.js";
import { workflowError } from "./store.js";

/** Resolve each role separately and preserve the provider's exact enforced Storage instance. */
export const buildEndpoint = Effect.fnUntraced(function* <E, R>(
  provider: Layer.Layer<Storage | TransferDriver, E, R>,
) {
  const context = yield* Layer.build(provider);
  const endpoint = Context.get(context, TransferDriver);
  if (Context.get(context, Storage) !== endpoint.storage)
    return yield* workflowError("MismatchedProviderLayer");
  return endpoint;
});
