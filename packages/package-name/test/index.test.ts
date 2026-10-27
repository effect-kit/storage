import { assert, it } from "@effect/vitest";
import { Effect } from "effect";

import { greet } from "../src/index.js";

it.effect("greets the supplied name", () =>
  Effect.gen(function* () {
    const greeting = yield* greet("Effect");
    assert.strictEqual(greeting, "Hello, Effect!");
  }),
);
