import { assert, it } from "@effect/vitest";

import { readCondition } from "../src/internal/read-condition.js";

it("uses weak comparison only for IfNoneMatch on an acquired representation", () => {
  assert.strictEqual(
    readCondition({ _tag: "IfNoneMatch", etag: 'W/"same"' }, '"same"'),
    "not-modified",
  );
  assert.strictEqual(readCondition({ _tag: "IfMatch", etag: 'W/"same"' }, 'W/"same"'), "failed");
  assert.strictEqual(readCondition({ _tag: "IfMatch", etag: '"old"' }, '"new"'), "failed");
  assert.strictEqual(readCondition({ _tag: "IfNoneMatch", etag: "*" }, undefined), "not-modified");
});
