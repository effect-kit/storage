import { Effect } from "effect";

import { TigrisAdministration, TigrisRecovery } from "@effect-kit/storage/tigris/native";
export const tigrisCapabilities = Effect.gen(function* () {
  const tigrisadministration = yield* TigrisAdministration;
  const tigrisrecovery = yield* TigrisRecovery;
  return [tigrisadministration, tigrisrecovery] as const;
});
