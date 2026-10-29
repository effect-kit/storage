import type { Redacted } from "effect";
import { Effect, Layer } from "effect";
import { KeyValueStore } from "effect/persistence";

import { UploadSessionStore } from "../sessions/store.js";
import type { CheckpointProtection, SessionOwnership } from "../sessions/store.js";
import { atomicFileStore } from "./filesystem.js";

export interface FileSessionStoreOptions {
  readonly directory: string;
  readonly namespace: string;
  readonly key: Redacted.Redacted<Uint8Array>;
  readonly ownership: "exclusive" | SessionOwnership;
  readonly maximumCheckpointBytes?: number;
}

export const fileSessionStore = <E, R>(
  options: FileSessionStoreOptions,
  protection: Effect.Effect<CheckpointProtection, E, R>,
) =>
  Layer.effect(
    UploadSessionStore,
    Effect.gen(function* () {
      const backing = yield* atomicFileStore(options.directory);
      const codec = yield* protection;
      return yield* UploadSessionStore.make({
        namespace: options.namespace,
        ownership: options.ownership,
        protection: codec,
        ...(options.maximumCheckpointBytes === undefined
          ? {}
          : { maximumCheckpointBytes: options.maximumCheckpointBytes }),
      }).pipe(Effect.provideService(KeyValueStore.KeyValueStore, backing));
    }),
  );
