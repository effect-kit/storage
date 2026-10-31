import { Layer } from "effect";

import { GcsStorage } from "../../../dist/gcs.js";
import * as Runtime from "../../../dist/node.js";
import { UploadSessions } from "../../../dist/sessions.js";
import { Transfers } from "../../../dist/transfers.js";

/** ADC/workload credentials are resolved and refreshed by the Google SDK. */
export function application(config, persistence, options = {}) {
  const provider = GcsStorage.layer(config);
  const transfers = Transfers.layer().pipe(Layer.provide(provider));
  const sessions = UploadSessions.layer({
    configurationId: options.configurationId ?? "gcs-upload-v1",
  }).pipe(Layer.provide(transfers), Layer.provide(Runtime.sessionStore(persistence)));
  return Layer.mergeAll(provider, transfers, sessions).pipe(Layer.provideMerge(Runtime.layer));
}
