import { Layer } from "effect";

import * as Runtime from "../../../dist/node.js";
import { UploadSessions } from "../../../dist/sessions.js";
import { SupabaseStorage } from "../../../dist/supabase.js";
import { Transfers } from "../../../dist/transfers.js";

export function application(config, persistence, options = {}) {
  const provider = SupabaseStorage.layer({ ...config, resumable: true });
  const transfers = Transfers.layer().pipe(Layer.provide(provider));
  const sessions = UploadSessions.layer({
    configurationId: options.configurationId ?? "supabase-tus-v1",
  }).pipe(Layer.provide(transfers), Layer.provide(Runtime.sessionStore(persistence)));
  return Layer.mergeAll(provider, transfers, sessions).pipe(Layer.provideMerge(Runtime.layer));
}
