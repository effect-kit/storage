import { assert, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import { ObjectKey, ObjectSize } from "../../src/index.js";

it("preserves cloud key identity without path or Unicode normalization", () => {
  for (const key of ["a//b", "../a", "/a/", "a%2Fb", "café", "cafe\u0301"]) {
    assert.strictEqual(Schema.decodeUnknownSync(ObjectKey)(key), key);
  }

  assert.throws(() => Schema.decodeUnknownSync(ObjectKey)(""));
});

it("rejects byte counts that cannot round-trip exactly through JSON", () => {
  for (const size of [-1, 0.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1, "5", 5n]) {
    assert.throws(() => Schema.decodeUnknownSync(ObjectSize)(size));
  }

  for (const size of [0, 5, Number.MAX_SAFE_INTEGER]) {
    assert.strictEqual(
      Schema.decodeUnknownSync(ObjectSize)(JSON.parse(JSON.stringify(size))),
      size,
    );
  }
});

it.effect("decodes object metadata at an Effect boundary", () =>
  Effect.gen(function* () {
    const metadata = yield* Schema.decodeUnknownEffect(
      Schema.Struct({ key: ObjectKey, size: ObjectSize }),
    )({ key: "invoices/example.pdf", size: 42 });

    assert.deepStrictEqual(metadata, { key: "invoices/example.pdf", size: 42 });
  }),
);
