import { assert, it } from "@effect/vitest";
import { Effect, Result, Schema } from "effect";

import { NativeVersion, ObjectMetadata } from "../../src/index.js";
import { MemoryStorage } from "../../src/testing/index.js";

it("native identity codecs preserve generations above the safe number range", () => {
  const encoded = {
    _tag: "GcsGeneration",
    id: "9007199254740993",
    metageneration: "9007199254740995",
  } as const;
  assert.deepStrictEqual(
    Schema.encodeSync(NativeVersion)(Schema.decodeUnknownSync(NativeVersion)(encoded)),
    encoded,
  );
  assert.throws(() =>
    Schema.decodeUnknownSync(NativeVersion)({ _tag: "GcsGeneration", id: 9007199254740992 }),
  );
});

it("metadata uses JSON-safe timestamp codecs and preserves checksum meaning", () => {
  const encoded = {
    ref: { storeId: "store", key: "a//b" },
    size: 0,
    lastModified: "2026-10-28T00:00:00.000Z",
    checksums: [
      {
        algorithm: "sha256",
        scope: "composite",
        encoding: "base64",
        value: "opaque",
        partCount: 2,
      },
    ],
  } as const;
  const metadata = Schema.decodeUnknownSync(ObjectMetadata)(encoded);
  assert.deepStrictEqual(Schema.encodeSync(ObjectMetadata)(metadata), encoded);
});

it.effect(
  "request validation rejects malformed keys, header injection and conflicting targets safely",
  () =>
    Effect.gen(function* () {
      const storage = yield* MemoryStorage.make();
      for (const request of [
        { key: "\ud800", body: new Uint8Array() },
        { key: "x\0y", body: new Uint8Array() },
        { key: "valid", body: new Uint8Array(), contentType: "text/plain\r\nSECRET: value" },
        {
          key: "valid",
          body: new Uint8Array(),
          condition: { _tag: "IfMatch" as const, etag: "SECRET\r\ninjected" },
        },
        { key: "valid", body: new Uint8Array(), customMetadata: { "invalid\nname": "SECRET" } },
      ]) {
        const result = yield* Effect.result(storage.put(request));
        assert.isTrue(Result.isFailure(result) && result.failure.reason._tag === "InvalidInput");
        if (Result.isFailure(result)) assert.notInclude(JSON.stringify(result.failure), "SECRET");
      }
      const contradictory = yield* Effect.result(
        storage.head({
          key: "valid",
          ref: { storeId: storage.storeId, key: "different" },
        } as never),
      );
      assert.isTrue(
        Result.isFailure(contradictory) && contradictory.failure.reason._tag === "InvalidInput",
      );
      const invalidBody = yield* Effect.result(
        storage.put({ key: "valid", body: "SECRET" } as never),
      );
      assert.isTrue(
        Result.isFailure(invalidBody) && invalidBody.failure.reason._tag === "InvalidInput",
      );
    }),
);

it.effect("wildcard conditions and degenerate ranges have precise outcomes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* MemoryStorage.make();
      yield* storage.put({ key: "wildcard", body: new Uint8Array([1]) });
      yield* storage.put({
        key: "wildcard",
        body: new Uint8Array([2]),
        condition: { _tag: "IfMatch", etag: "*" },
      });
      yield* storage.get({ key: "wildcard", condition: { _tag: "IfMatch", etag: "*" } });
      for (const range of [
        { _tag: "Bounded", start: 1, end: 0 },
        { _tag: "From", start: 1 },
      ] as const) {
        const result = yield* Effect.result(storage.get({ key: "wildcard", range }));
        assert.isTrue(
          Result.isFailure(result) && result.failure.reason._tag === "RangeNotSatisfiable",
        );
      }
      yield* storage.put({ key: "empty", body: new Uint8Array() });
      const emptyRange = yield* Effect.result(
        storage.get({ key: "empty", range: { _tag: "Suffix", length: 1 } }),
      );
      assert.isTrue(
        Result.isFailure(emptyRange) && emptyRange.failure.reason._tag === "RangeNotSatisfiable",
      );
      yield* storage.delete({ key: "wildcard", condition: { _tag: "IfMatch", etag: "*" } });
      const missing = yield* Effect.result(
        storage.delete({ key: "wildcard", condition: { _tag: "IfMatch", etag: "*" } }),
      );
      assert.isTrue(
        Result.isFailure(missing) && missing.failure.reason._tag === "PreconditionFailed",
      );
    }),
  ),
);
