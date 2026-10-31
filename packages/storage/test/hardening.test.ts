import { assert, it } from "@effect/vitest";
import { Effect, Result } from "effect";

import { StoragePrefix } from "../src/composition.js";
import type { GetRequest, PutRequest } from "../src/requests.js";
import { Storage } from "../src/storage.js";
import { MemoryStorage } from "../src/testing.js";

const canary = "private-request-body-and-token";

it.effect("hostile request boundaries fail before provider I/O and redact invalid values", () =>
  Effect.gen(function* () {
    const base = yield* MemoryStorage.make();
    let writes = 0;
    const storage = Storage.make({
      ...base,
      put: (request) => {
        writes++;
        return base.put(request);
      },
    });
    const invalid = ["", "\0", "\ud800", "\udfff"];
    for (let index = 0; index < 256; index++) {
      const key = index % 4 === 0 ? "" : `${canary}-${index}${invalid[index % invalid.length]}`;
      const result = yield* Effect.result(
        storage.put({ key, body: new TextEncoder().encode(canary) }),
      );
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) assert.notInclude(JSON.stringify(result.failure), canary);
    }
    for (const value of ["\r", "\n", "\0", "\ud800"]) {
      const request = { key: "valid", body: new Uint8Array(), contentType: `${canary}${value}` };
      const result = yield* Effect.result(storage.put(request as PutRequest));
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) assert.notInclude(JSON.stringify(result.failure), canary);
    }
    assert.strictEqual(writes, 0);
  }),
);

it.effect("seeded range probes return exact bytes or an explicit range failure", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* MemoryStorage.make();
      const bytes = new Uint8Array(257).map((_, index) => index % 251);
      yield* storage.put({ key: "range", body: bytes });
      let seed = 0x5eed123;
      const next = () => {
        seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
        return seed % 600;
      };
      for (let index = 0; index < 512; index++) {
        const start = next();
        const end = next();
        const result = yield* Effect.result(
          Storage.getBytes({
            key: "range",
            range: { _tag: "Bounded", start, end },
            maxBytes: bytes.length,
          }).pipe(Effect.provideService(Storage, storage)),
        );
        if (start >= bytes.length || end < start) {
          assert.isTrue(Result.isFailure(result));
        } else {
          assert.isTrue(Result.isSuccess(result));
          if (Result.isSuccess(result))
            assert.deepStrictEqual(
              result.success,
              bytes.slice(start, Math.min(end + 1, bytes.length)),
            );
        }
      }
      for (const range of [
        { _tag: "From", start: -1 },
        { _tag: "Suffix", length: 0 },
        { _tag: "Bounded", start: NaN, end: Infinity },
      ]) {
        const result = yield* Effect.result(storage.get({ key: "range", range } as GetRequest));
        assert.isTrue(Result.isFailure(result));
      }
    }),
  ),
);

it.effect("tenant views reject foreign references and hostile continuation tokens", () =>
  Effect.gen(function* () {
    const backing = yield* MemoryStorage.make();
    const view = yield* StoragePrefix.make({ prefix: "tenant-a/" }).pipe(
      Effect.provideService(Storage, backing),
    );
    const other = yield* StoragePrefix.make({ prefix: "tenant-b/" }).pipe(
      Effect.provideService(Storage, backing),
    );
    const secret = yield* other.put({ key: "secret", body: new TextEncoder().encode(canary) });
    assert.isTrue(Result.isFailure(yield* Effect.result(view.head({ ref: secret.ref }))));
    assert.isTrue(Result.isFailure(yield* Effect.result(view.delete({ ref: secret.ref }))));
    for (let index = 0; index < 128; index++) {
      const cursor = JSON.stringify({ storeId: other.storeId, query: `${index}`, cursor: canary });
      const result = yield* Effect.result(view.listPage({ cursor, limit: 1 }));
      assert.isTrue(Result.isFailure(result));
      if (Result.isFailure(result)) assert.notInclude(JSON.stringify(result.failure), canary);
    }
    assert.strictEqual((yield* other.head({ ref: secret.ref })).size, canary.length);
  }),
);
