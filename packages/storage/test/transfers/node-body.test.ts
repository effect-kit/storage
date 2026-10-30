import { assert, it } from "@effect/vitest";
import { Effect, Stream } from "effect";

import { uploadBody } from "../../src/internal/node/upload-body.js";

it.effect("a provider acknowledgement cannot hide the upload source's final error", () =>
  Effect.gen(function* () {
    const source = Stream.concat(
      Stream.succeed(new Uint8Array([1])),
      Stream.fail("source-failed" as const),
    );
    const result = yield* uploadBody(
      { provider: "test", maxUnknownLengthBytes: 8 },
      { body: source, contentLength: 1 },
      "put",
      () => Effect.succeed("acknowledged"),
    ).pipe(Effect.result);
    assert.strictEqual(result._tag === "Failure" && result.failure, "source-failed");
  }),
);
