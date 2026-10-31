import { assert, it } from "@effect/vitest";
import { Effect, Stream } from "effect";

import { storageError } from "../../src/core/errors.js";
import { webUploadBody } from "../../src/internal/web-upload-body.js";

it.effect("preserves a late source error after a provider acknowledgement", () =>
  Effect.gen(function* () {
    const source = Stream.concat(
      Stream.succeed(new Uint8Array([1])),
      Stream.fail("source-failure" as const),
    );
    const result = yield* webUploadBody("fixture", { body: source, contentLength: 1 }, 8, (body) =>
      Effect.tryPromise({
        try: async () => {
          if (body instanceof Uint8Array) return;
          const reader = body.getReader();
          await reader.read();
          reader.releaseLock();
        },
        catch: () => storageError("put", "fixture", { _tag: "TransportFailure" }),
      }),
    ).pipe(Effect.result);
    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") assert.strictEqual(result.failure, "source-failure");
  }),
);

it.effect("rejects bounded unknown streams before starting provider work", () =>
  Effect.gen(function* () {
    let called = false;
    const result = yield* webUploadBody(
      "fixture",
      { body: Stream.succeed(new Uint8Array(9)) },
      8,
      () =>
        Effect.sync(() => {
          called = true;
        }),
    ).pipe(Effect.result);
    assert.isFalse(called);
    assert.strictEqual(result._tag === "Failure" && result.failure.reason._tag, "LimitExceeded");
  }),
);

it.effect("detects short known streams and releases a pending consumer", () =>
  Effect.gen(function* () {
    const result = yield* webUploadBody(
      "fixture",
      { body: Stream.succeed(new Uint8Array([1])), contentLength: 2 },
      8,
      (body) =>
        Effect.tryPromise({
          try: () => new Response(body as ReadableStream<Uint8Array>).arrayBuffer(),
          catch: () => storageError("put", "fixture", { _tag: "TransportFailure" }),
        }),
    ).pipe(Effect.result);
    assert.strictEqual(result._tag === "Failure" && result.failure.reason._tag, "LengthMismatch");
  }),
);
