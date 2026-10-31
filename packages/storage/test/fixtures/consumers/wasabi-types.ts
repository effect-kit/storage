import { Effect } from "effect";

import { WasabiNative, WasabiCompliance } from "@effect-kit/storage/wasabi/native";
export const wasabiCapabilities = Effect.gen(function* () {
  const wasabinative = yield* WasabiNative;
  const wasabicompliance = yield* WasabiCompliance;
  return [wasabinative, wasabicompliance] as const;
});
