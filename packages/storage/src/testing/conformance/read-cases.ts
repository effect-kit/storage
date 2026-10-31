import { Effect, Stream } from "effect";

import { Storage } from "../../core/storage.js";
import type { StorageConformanceCase } from "../conformance.js";
import { bytesEqual, check, expectReason } from "./assertions.js";

export const readCases: ReadonlyArray<StorageConformanceCase> = [
  {
    name: "single-open-and-suffix-ranges",
    requires: (profile) =>
      ["bounded", "from", "suffix"].every((kind) => profile.ranges.some((range) => range === kind)),
    run: (storage) =>
      Effect.gen(function* () {
        yield* storage.put({ key: "range", body: new Uint8Array([0, 1, 2, 3]) });
        for (const [range, expected] of [
          [{ _tag: "Bounded", start: 1, end: 2 }, [1, 2]],
          [{ _tag: "From", start: 2 }, [2, 3]],
          [{ _tag: "Suffix", length: 2 }, [2, 3]],
        ] as const) {
          const actual = yield* Storage.getBytes({ key: "range", range, maxBytes: 4 }).pipe(
            Effect.provideService(Storage, storage),
          );
          yield* check(bytesEqual(actual, new Uint8Array(expected)), "Wrong range bytes");
        }
        yield* expectReason(
          storage.get({ key: "range", range: { _tag: "From", start: 4 } }),
          "RangeNotSatisfiable",
        );
      }),
  },
  {
    name: "not-modified-is-not-an-empty-body",
    requires: (profile) => profile.getConditions.includes("if-none-match"),
    run: (storage) =>
      Effect.gen(function* () {
        yield* storage.put({ key: "cached", body: new Uint8Array([1]) });
        const response = yield* storage.get({
          key: "cached",
          condition: { _tag: "IfNoneMatch", etag: "*" },
        });
        yield* check(
          response._tag === "NotModified" && !("body" in response),
          "Conditional read fabricated a body",
        );
      }),
  },
  {
    name: "body-cannot-outlive-acquisition",
    run: (storage) =>
      Effect.gen(function* () {
        yield* storage.put({ key: "scoped", body: new Uint8Array([1]) });
        const response = yield* Effect.scoped(storage.get({ key: "scoped" }));
        if (response._tag !== "Object")
          return yield* check(false, "Unconditional get did not return an object");
        yield* expectReason(Stream.runDrain(response.body), "ScopeClosed");
      }),
  },
  {
    name: "explicit-collection-limit",
    run: (storage) =>
      Effect.gen(function* () {
        yield* storage.put({ key: "large", body: new Uint8Array([1, 2, 3]) });
        yield* expectReason(
          Storage.getBytes({ key: "large", maxBytes: 2 }).pipe(
            Effect.provideService(Storage, storage),
          ),
          "LimitExceeded",
        );
      }),
  },
];
