import { assert, it } from "@effect/vitest";
import { Effect } from "effect";

import { MemoryStorage } from "../../../src/testing/index.js";
import { safetySuite } from "../../e2e/live/safety.js";

it.effect("live safety suite proves concurrent conditions and cleans every observed version", () =>
  Effect.gen(function* () {
    const storage = yield* MemoryStorage.make();
    const report = { checks: [] as string[], cleanupPending: 0 };
    yield* Effect.scoped(safetySuite(storage, "effect-storage-acceptance/unit/", report));

    assert.include(report.checks, "concurrent-create-only");
    assert.include(report.checks, "stale-write-rejected");
    assert.equal(report.cleanupPending, 0);
    const remaining = yield* storage.listPage();
    assert.equal(remaining.entries.length, 0);
  }),
);
