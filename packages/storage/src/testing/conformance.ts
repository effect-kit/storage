import { Effect } from "effect";
import type { Scope } from "effect";

import type { StorageCapabilities } from "../capabilities.js";
import type { StorageError } from "../errors.js";
import type { StorageService } from "../storage.js";
import { ConformanceFailure } from "./conformance/assertions.js";
import { coreCases } from "./conformance/core-cases.js";
import { listCases } from "./conformance/list-cases.js";
import { readCases } from "./conformance/read-cases.js";

export { ConformanceFailure } from "./conformance/assertions.js";

export interface StorageConformanceCase {
  readonly name: string;
  readonly requires?: ((profile: StorageCapabilities) => boolean) | undefined;
  readonly run: (
    storage: StorageService,
  ) => Effect.Effect<void, StorageError | ConformanceFailure, Scope.Scope>;
}

export const storageConformanceCases: ReadonlyArray<StorageConformanceCase> = [
  ...coreCases,
  ...readCases,
  ...listCases,
];

export type ConformanceResult =
  | { readonly name: string; readonly status: "passed" }
  | { readonly name: string; readonly status: "skipped"; readonly reason: string };

/** Each case owns a fresh scoped fixture; callers own provider resource cleanup. */
export function runStorageConformance<E, R>(
  makeFixture: () => Effect.Effect<StorageService, E, R>,
): Effect.Effect<
  ReadonlyArray<ConformanceResult>,
  E | ConformanceFailure,
  Exclude<R, Scope.Scope>
> {
  return Effect.forEach(storageConformanceCases, (test) =>
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* makeFixture();
        if (test.requires && !test.requires(storage.capabilities)) {
          return {
            name: test.name,
            status: "skipped",
            reason: "Capability is not advertised by this profile",
          } as const;
        }
        yield* test.run(storage).pipe(
          Effect.catchTag("StorageError", (error) =>
            Effect.fail(new ConformanceFailure({ testName: test.name, detail: error.reason._tag })),
          ),
          Effect.catchTag("ConformanceFailure", (error) =>
            Effect.fail(new ConformanceFailure({ testName: test.name, detail: error.detail })),
          ),
        );
        return { name: test.name, status: "passed" } as const;
      }),
    ),
  );
}
