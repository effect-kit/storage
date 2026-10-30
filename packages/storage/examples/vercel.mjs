import { Layer } from "effect";

import * as Runtime from "../dist/node.js";
import { Transfers } from "../dist/transfers.js";
import { VercelStorage } from "../dist/vercel.js";
import { VercelSigning } from "../dist/vercel/signing.js";

export function application(config) {
  const provider = VercelStorage.layer(config);
  const transfers = Transfers.layer().pipe(Layer.provide(provider));
  const signing = VercelSigning.layer().pipe(Layer.provide(provider));
  return Layer.mergeAll(provider, transfers, signing).pipe(Layer.provideMerge(Runtime.layer));
}
