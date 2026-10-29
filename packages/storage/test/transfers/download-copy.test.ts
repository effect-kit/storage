import { assert, it } from "@effect/vitest";
import { Effect, Result, Stream } from "effect";

import { storageError } from "../../src/errors.js";
import { Storage } from "../../src/index.js";
import { UploadSource } from "../../src/transfers.js";
import { fixture, policy, makeTransfers } from "./fixture.js";

it.effect("continues a failed download from acknowledged bytes using the pinned reference", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const metadata = yield* test.endpoint.storage.put({
      key: "source",
      body: new Uint8Array([1, 2, 3, 4]),
    });
    const reads: unknown[] = [];
    const storage = Storage.make({
      ...test.endpoint.storage,
      get: (request) =>
        test.endpoint.storage.get(request).pipe(
          Effect.map((response) => {
            reads.push(request);
            if (response._tag !== "Object" || reads.length > 1) return response;
            return {
              ...response,
              body: Stream.concat(
                Stream.succeed(new Uint8Array([1, 2])),
                Stream.fail(storageError("get", "test", { _tag: "TransportFailure" })),
              ),
            };
          }),
        ),
    });
    const transfers = yield* makeTransfers({ ...test.endpoint, storage }, policy);
    const output: number[] = [];
    const receipt = yield* transfers.download({
      key: "source",
      target: {
        write: (bytes) =>
          Effect.sync(() => {
            output.push(...bytes);
          }),
      },
    });
    assert.deepEqual(output, [1, 2, 3, 4]);
    assert.strictEqual(receipt.progress.bytesVerified, 4);
    assert.deepEqual(reads[1], { ref: metadata.ref, range: { _tag: "From", start: 2 } });
  }),
);

it.effect("rejects replacement during recovery and never retries a failed target write", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    yield* test.endpoint.storage.put({ key: "source", body: new Uint8Array([1, 2, 3, 4]) });
    let calls = 0;
    const storage = Storage.make({
      ...test.endpoint.storage,
      get: (request) =>
        test.endpoint.storage.get(request).pipe(
          Effect.map((response) => {
            calls++;
            if (response._tag !== "Object" || calls > 1) return response;
            return {
              ...response,
              body: Stream.concat(
                Stream.succeed(new Uint8Array([1, 2])),
                Stream.unwrap(
                  test.endpoint.storage
                    .put({ key: "source", body: new Uint8Array([9, 9, 9, 9]) })
                    .pipe(
                      Effect.as(
                        Stream.fail(storageError("get", "test", { _tag: "TransportFailure" })),
                      ),
                    ),
                ),
              ),
            };
          }),
        ),
    });
    const transfers = yield* makeTransfers({ ...test.endpoint, storage }, policy);
    const output: number[] = [];
    const changed = yield* Effect.result(
      transfers.download({
        key: "source",
        target: {
          write: (bytes) =>
            Effect.sync(() => {
              output.push(...bytes);
            }),
        },
      }),
    );
    assert.isTrue(Result.isFailure(changed));
    assert.deepEqual(output, [1, 2]);

    calls = 0;
    const sinkError = storageError("sink", "test", { _tag: "TransportFailure" });
    const failed = yield* Effect.result(
      transfers.download({ key: "source", target: { write: () => Effect.fail(sinkError) } }),
    );
    assert.isTrue(Result.isFailure(failed) && failed.failure === sinkError);
    assert.strictEqual(calls, 1);
  }),
);

it.effect("detects actual download length and full-object checksum mismatch", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    yield* test.endpoint.storage.put({ key: "source", body: new Uint8Array([1, 2, 3]) });
    const transfers = yield* makeTransfers(test.endpoint, policy);
    const mismatch = yield* Effect.result(
      transfers.download({
        key: "source",
        target: { write: () => Effect.void },
        expectedChecksum: {
          algorithm: "sha256",
          scope: "full-object",
          encoding: "hex",
          value: "00",
        },
      }),
    );
    assert.isTrue(
      Result.isFailure(mismatch) && mismatch.failure.reason._tag === "IntegrityMismatch",
    );
    const storage = Storage.make({
      ...test.endpoint.storage,
      get: (request) =>
        test.endpoint.storage
          .get(request)
          .pipe(
            Effect.map((response) =>
              response._tag === "Object" ? { ...response, contentLength: 2 } : response,
            ),
          ),
    });
    const lying = yield* makeTransfers({ ...test.endpoint, storage }, policy);
    const result = yield* Effect.result(
      lying.download({ key: "source", target: { write: () => Effect.void } }),
    );
    assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "LengthMismatch");
  }),
);

it.effect("streams a verified cross-store copy and reports copy-success/delete-failure", () =>
  Effect.gen(function* () {
    const from = yield* fixture();
    const to = yield* fixture();
    const source = yield* from.endpoint.storage.put({
      key: "source",
      body: new Uint8Array(48).fill(7),
    });
    const transfers = yield* makeTransfers(to.endpoint, policy);
    const receipt = yield* transfers.copy({
      source: from.endpoint,
      ref: source.ref,
      destinationKey: "copied",
    });
    assert.strictEqual(receipt.strategy, "streamed-copy");
    assert.strictEqual(receipt.progress.bytesVerified, 48);

    const failDelete = {
      ...from.endpoint,
      storage: Storage.make({
        ...from.endpoint.storage,
        delete: () =>
          Effect.fail(storageError("delete", "test", { _tag: "AccessDenied" }, "rejected")),
      }),
    };
    const move = yield* transfers.move({
      source: failDelete,
      ref: source.ref,
      destinationKey: "moved",
    });
    assert.strictEqual(move._tag, "Copied");
    assert.strictEqual((yield* to.endpoint.storage.head({ key: "moved" })).size, 48);
    assert.strictEqual((yield* from.endpoint.storage.head({ ref: source.ref })).size, 48);
  }),
);

it.effect("lazy bulk stops enumeration on cancellation and keeps per-entry failures ordered", () =>
  Effect.gen(function* () {
    const test = yield* fixture();
    const transfers = yield* makeTransfers({ ...test.endpoint, multipart: undefined }, policy);
    let enumerated = 0;
    const requests = Stream.fromIterable(
      (function* () {
        for (let index = 0; index < 1000; index++) {
          enumerated++;
          yield {
            key: `bulk-${index}`,
            source: UploadSource.bytes(new Uint8Array(index === 1 ? 513 : 1)),
          };
        }
      })(),
      { chunkSize: 1 },
    );
    const results = yield* transfers
      .uploadMany(requests, 2)
      .pipe(Stream.take(3), Stream.runCollect);
    assert.strictEqual(results.length, 3);
    assert.isTrue(Result.isSuccess(results[0] as (typeof results)[number]));
    assert.isTrue(Result.isFailure(results[1] as (typeof results)[number]));
    assert.isTrue(Result.isSuccess(results[2] as (typeof results)[number]));
    assert.isAtMost(enumerated, 6);
    assert.strictEqual((yield* transfers.resources).reservedBytes, 0);
  }),
);
