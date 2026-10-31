import { assert, it } from "@effect/vitest";
import { Cause, Context, Effect, Exit, Fiber, Result, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";

import { HeadObjectCommand } from "@aws-sdk/client-s3";
import { vi } from "vitest";

import { S3Storage } from "../../../src/providers/s3/index.js";
import {
  client,
  drain,
  fixtureCredentials,
  httpFixture,
  xml,
} from "../../fixtures/s3/http-fixture.js";

class SourceFailure extends Schema.TaggedError<SourceFailure>()("SourceFailure", {}) {}
class SourceBytes extends Context.Service<SourceBytes, Uint8Array>()("s3-test/source-bytes") {}

it.effect("owned clients are destroyed exactly once at scope release", () =>
  Effect.gen(function* () {
    let destroyed = 0;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* S3Storage.make({
          bucket: "owned-client-test",
          region: "us-east-1",
          credentials: fixtureCredentials(),
        });
        const original = storage.client.destroy.bind(storage.client);
        storage.client.destroy = () => {
          destroyed++;
          original();
        };
        assert.strictEqual(destroyed, 0);
      }),
    );
    assert.strictEqual(destroyed, 1);
  }),
);

it.effect(
  "known-length streams preserve caller services and failures and release their source",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture((incoming, response) => {
          void drain(incoming).then(
            () => {
              response.writeHead(200, { etag: '"ok"' });
              return response.end();
            },
            () => {
              response.destroy();
            },
          );
        });
        let released = 0;
        const failure = new SourceFailure();
        const source = Stream.unwrap(
          Effect.acquireRelease(Effect.succeed(undefined), () =>
            Effect.sync(() => {
              released++;
            }),
          ).pipe(Effect.as(Stream.concat(Stream.fromEffect(SourceBytes), Stream.fail(failure)))),
        );
        const result = yield* Effect.result(
          fixture.storage
            .put({ key: "source", body: source, contentLength: 4 })
            .pipe(Effect.provideService(SourceBytes, new Uint8Array([1]))),
        );
        assert.isTrue(Result.isFailure(result) && result.failure === failure);
        assert.strictEqual(released, 1);
        const defect = yield* Effect.exit(
          fixture.storage.put({
            key: "defect",
            contentLength: 2,
            body: Stream.die("source-defect"),
          }),
        );
        assert.isTrue(Exit.isFailure(defect) && Cause.hasDies(defect.cause));
      }),
    ),
);

it.effect(
  "unknown-length staging is bounded and a failed staged source never sends a request",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0;
        const fixture = yield* httpFixture((_incoming, response) => {
          calls++;
          return response.end();
        });
        const storage = yield* S3Storage.make({ ...fixture.config, maxUnknownLengthBytes: 2 });
        const result = yield* Effect.result(
          storage.put({ key: "large", body: Stream.make(new Uint8Array(2), new Uint8Array(1)) }),
        );
        assert.isTrue(
          Result.isFailure(result) &&
            result.failure.reason._tag === "LimitExceeded" &&
            result.failure.mutationOutcome === "not-started",
        );
        assert.strictEqual(calls, 0);
      }),
    ),
);

it.effect("interruption aborts an outstanding request and a deadline is an unknown mutation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let entered: (() => void) | undefined;
      const started = new Promise<void>((resolve) => {
        entered = resolve;
      });
      let closed: (() => void) | undefined;
      const stopped = new Promise<void>((resolve) => {
        closed = resolve;
      });
      const fixture = yield* httpFixture((incoming, response) => {
        incoming.on("close", () => {
          closed?.();
          response.destroy();
        });
        entered?.();
      });
      const fiber = yield* Effect.forkChild(fixture.storage.head({ key: "pending" }));
      yield* Effect.promise(() => started);
      yield* Fiber.interrupt(fiber);
      yield* Effect.promise(() => stopped);

      const storage = yield* S3Storage.make({ ...fixture.config, requestTimeoutMillis: 10 });
      const pending = yield* Effect.forkChild(
        Effect.result(storage.put({ key: "timeout", body: new Uint8Array() })),
      );
      yield* TestClock.adjust(10);
      const result = yield* Fiber.join(pending);
      assert.isTrue(Result.isFailure(result) && result.failure.mutationOutcome === "unknown");
    }),
  ),
);

it.effect("unread and partial SDK bodies are destroyed when their scopes close", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture((_incoming, response) => {
        response.writeHead(200, { "content-length": String(1024 * 1024) });
        response.write(new Uint8Array(1024));
      });
      const nativeBodies: Array<{ destroyed: boolean }> = [];
      fixture.storage.client.middlewareStack.add(
        (next) => async (args) => {
          const result = await next(args);
          if (result.output && "Body" in result.output)
            nativeBodies.push(result.output.Body as { destroyed: boolean });
          return result;
        },
        { name: "captureBody", step: "deserialize", priority: "high" },
      );
      yield* Effect.scoped(fixture.storage.get({ key: "unread" }));
      yield* Effect.scoped(
        Effect.gen(function* () {
          const response = yield* fixture.storage.get({ key: "partial" });
          if (response._tag === "Object")
            yield* Stream.runDrain(response.body.pipe(Stream.take(1)));
        }),
      );
      assert.strictEqual(nativeBodies.length, 2);
      assert.isTrue(nativeBodies.every((body) => body.destroyed));
    }),
  ),
);

it.effect(
  "borrowed clients stay owned by the caller and incompatible retry policy is rejected",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* httpFixture((_incoming, response) => {
          response.writeHead(200, { "content-length": "0" });
          return response.end();
        });
        const borrowed = client(fixture.config);
        const destroy = vi.spyOn(borrowed, "destroy");
        const { credentials: _credentials, ...borrowedConfig } = fixture.config;
        yield* Effect.scoped(
          S3Storage.makeClient(borrowed, { ...borrowedConfig, storeId: "borrowed" }),
        );
        assert.strictEqual(destroy.mock.calls.length, 0);
        yield* Effect.promise(() =>
          borrowed.send(
            new HeadObjectCommand({ Bucket: fixture.config.bucket, Key: "still-open" }),
          ),
        );
        borrowed.destroy();
        const bad = client(fixture.config);
        bad.config.maxAttempts = async () => 3;
        const invalid = yield* Effect.result(
          S3Storage.makeClient(bad, { ...borrowedConfig, storeId: "bad" }),
        );
        assert.isTrue(
          Result.isFailure(invalid) && invalid.failure.reason._tag === "InvalidConfiguration",
        );
        bad.destroy();
      }),
    ),
);

it.effect("credentials are resolved again when their SDK lifetime requires refresh", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let refreshes = 0;
      const fixture = yield* httpFixture((_incoming, response) => {
        response.writeHead(200, { "content-length": "0" });
        response.end();
      });
      const identity = fixtureCredentials();
      const storage = yield* S3Storage.make({
        ...fixture.config,
        credentials: async () => {
          refreshes++;
          return { ...identity, expiration: new Date(Date.now() + 1000) };
        },
      });
      yield* storage.head({ key: "first" });
      yield* storage.head({ key: "second" });
      assert.isAtLeast(refreshes, 2);
    }),
  ),
);

it.effect("a lost mutation response remains unknown with exactly one transport attempt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let attempts = 0;
      const fixture = yield* httpFixture((incoming) => {
        attempts++;
        void drain(incoming).then(() => {
          return incoming.socket.destroy();
        });
      });
      const result = yield* Effect.result(
        fixture.storage.put({ key: "committed-maybe", body: new Uint8Array([1]) }),
      );
      assert.isTrue(Result.isFailure(result) && result.failure.mutationOutcome === "unknown");
      assert.strictEqual(attempts, 1);
    }),
  ),
);

it.effect("wrong region and native checksum rejection retain safe provider information", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture((incoming, response) => {
        void drain(incoming).then(() =>
          xml(
            response,
            incoming.url?.includes("checksum")
              ? "<Error><Code>BadDigest</Code><Message>SECRET</Message></Error>"
              : "<Error><Code>AuthorizationHeaderMalformed</Code><Message>SECRET</Message></Error>",
            400,
          ),
        );
      });
      for (const key of ["region", "checksum"]) {
        const result = yield* Effect.result(fixture.storage.put({ key, body: new Uint8Array() }));
        assert.isTrue(Result.isFailure(result) && result.failure.mutationOutcome === "rejected");
        if (Result.isFailure(result))
          assert.strictEqual(
            result.failure.reason._tag,
            key === "checksum" ? "IntegrityMismatch" : "ProviderFailure",
          );
        assert.notInclude(JSON.stringify(result), "SECRET");
      }
    }),
  ),
);
