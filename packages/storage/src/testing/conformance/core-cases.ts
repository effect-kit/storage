import { Effect, Result, Stream } from "effect";

import type { StorageConformanceCase } from "../conformance.js";
import { bytesEqual, check, expectReason, read } from "./assertions.js";

export const coreCases: ReadonlyArray<StorageConformanceCase> = [
  {
    name: "binary-empty-and-metadata",
    run: (storage) =>
      Effect.gen(function* () {
        const expected = new Uint8Array([0, 255, 13, 10]);
        const saved = yield* storage.put({
          key: "binary",
          body: expected,
          contentType: "application/octet-stream",
          customMetadata: { suite: "portable" },
        });
        const head = yield* storage.head({ key: "binary" });
        yield* check(
          head.ref.storeId === storage.storeId && head.ref.key === "binary",
          "Incorrect namespace identity",
        );
        yield* check(
          head.contentType === "application/octet-stream" &&
            head.customMetadata?.suite === "portable",
          "Metadata did not round-trip",
        );
        yield* check(
          saved.etag === undefined || saved.etag === head.etag,
          "Write/head identity differs without a mutation",
        );
        yield* check(bytesEqual(yield* read(storage, "binary"), expected), "Binary bytes changed");
        yield* storage.put({ key: "empty", body: new Uint8Array() });
        yield* check(
          (yield* read(storage, "empty")).length === 0,
          "Empty object did not round-trip",
        );
      }),
  },
  {
    name: "stream-and-overwrite",
    run: (storage) =>
      Effect.gen(function* () {
        yield* storage.put({
          key: "stream",
          body: Stream.make(new Uint8Array([1]), new Uint8Array([2])),
        });
        yield* check(
          bytesEqual(yield* read(storage, "stream"), new Uint8Array([1, 2])),
          "Stream bytes changed",
        );
        yield* storage.put({ key: "stream", body: new Uint8Array([3]) });
        yield* check(
          bytesEqual(yield* read(storage, "stream"), new Uint8Array([3])),
          "Overwrite did not replace",
        );
      }),
  },
  {
    name: "missing-and-idempotent-delete",
    run: (storage) =>
      Effect.gen(function* () {
        yield* expectReason(storage.head({ key: "missing" }), "NotFound");
        yield* expectReason(storage.get({ key: "missing" }), "NotFound");
        yield* storage.delete({ key: "missing" });
        yield* storage.put({ key: "remove", body: new Uint8Array([1]) });
        yield* storage.delete({ key: "remove" });
        yield* storage.delete({ key: "remove" });
        yield* expectReason(storage.head({ key: "remove" }), "NotFound");
      }),
  },
  {
    name: "atomic-create-only",
    requires: (profile) => profile.putConditions.includes("create-only"),
    run: (storage) =>
      Effect.gen(function* () {
        const results = yield* Effect.all(
          [1, 2].map((byte) =>
            Effect.result(
              storage.put({
                key: "race",
                body: new Uint8Array([byte]),
                condition: { _tag: "CreateOnly" },
              }),
            ),
          ),
          { concurrency: 2 },
        );
        yield* check(
          results.filter(Result.isSuccess).length === 1,
          "Create-only did not have one winner",
        );
        const failed = results.find(Result.isFailure);
        yield* check(
          failed?.failure.reason._tag === "PreconditionFailed",
          "Losing writer was not conditionally rejected",
        );
      }),
  },
  {
    name: "stale-write-condition",
    requires: (profile) => profile.putConditions.includes("if-match"),
    run: (storage) =>
      Effect.gen(function* () {
        const first = yield* storage.put({ key: "match", body: new Uint8Array([1]) });
        yield* check(first.etag !== undefined, "Conditional adapter omitted its validator");
        yield* expectReason(
          storage.put({
            key: "match",
            body: new Uint8Array([2]),
            condition: { _tag: "IfMatch", etag: '"deliberately-stale"' },
          }),
          "PreconditionFailed",
        );
        yield* check(
          bytesEqual(yield* read(storage, "match"), new Uint8Array([1])),
          "Rejected write changed bytes",
        );
      }),
  },
  {
    name: "foreign-reference",
    run: (storage) =>
      expectReason(
        storage.head({
          ref: { storeId: `foreign:${storage.storeId}`, key: "anything" },
        }),
        "InvalidInput",
      ),
  },
];
