import { assert, describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { MemoryStorage, runStorageConformance, storageConformanceCases } from "../src/testing.js";

describe("memory portable conformance", () => {
  for (const test of storageConformanceCases) {
    it.effect(test.name, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* MemoryStorage.make();
          yield* test.run(storage);
        }),
      ),
    );
  }
});

it.effect("the public harness scopes a fresh caller-owned fixture per case", () =>
  Effect.gen(function* () {
    let acquired = 0;
    let released = 0;
    const results = yield* runStorageConformance(() =>
      Effect.acquireRelease(
        Effect.gen(function* () {
          acquired++;
          return yield* MemoryStorage.make();
        }),
        () =>
          Effect.sync(() => {
            released++;
          }),
      ),
    );
    assert.strictEqual(acquired, storageConformanceCases.length);
    assert.strictEqual(released, acquired);
    assert.isTrue(results.every((result) => result.status === "passed"));
  }),
);
