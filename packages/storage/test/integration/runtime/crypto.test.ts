import { randomBytes } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Effect, Redacted } from "effect";

import * as BrowserStorage from "../../../src/runtime/browser.js";
import { aes256Gcm as nativeProtection, sha256 as nativeHash } from "../../../src/runtime/node.js";

it.effect(
  "native and web checkpoint protection can read each other's authenticated envelopes",
  () =>
    Effect.gen(function* () {
      const secret = Redacted.make(randomBytes(32));
      const native = nativeProtection(secret);
      const web = yield* BrowserStorage.aes256Gcm(secret);
      const sealedNative = yield* native.seal("protected handle", "tenant:session");
      const sealedWeb = yield* web.seal("protected handle", "tenant:session");
      assert.strictEqual(yield* web.open(sealedNative, "tenant:session"), "protected handle");
      assert.strictEqual(yield* native.open(sealedWeb, "tenant:session"), "protected handle");
    }).pipe(Effect.provide(BrowserStorage.layer)),
);

it("portable hashing matches native hashing across empty inputs, backing-buffer views and snapshots", () => {
  for (const bytes of [new Uint8Array(), randomBytes(131071).subarray(7, 100003)]) {
    const native = nativeHash.create();
    const portable = BrowserStorage.sha256.create();
    for (let offset = 0; offset < bytes.length; offset += 65537) {
      const chunk = bytes.subarray(offset, offset + 65537);
      native.update(chunk);
      portable.update(chunk);
      assert.deepEqual(portable.snapshot?.(), native.snapshot?.());
    }
    assert.deepEqual(portable.finish(), native.finish());
  }
});
