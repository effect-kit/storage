import { assert, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Stream } from "effect";
import { FetchHttpClient } from "effect/http";

import { StorageSigning } from "../../src/capabilities.js";
import { Storage } from "../../src/storage.js";
import { SupabaseStorage } from "../../src/supabase.js";
import { SupabaseSigning, SupabaseUploadGrants } from "../../src/supabase/signing.js";
import { runStorageConformance } from "../../src/testing.js";
import { httpFixture } from "./http-fixture.js";

it.effect("runs native REST core contracts with same-response validators and ranges", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const make = () =>
        SupabaseStorage.make({
          url: fixture.url,
          bucket: "bucket",
          storeId: "supabase:test",
          auth: { mode: "user", token: Redacted.make("user-a") },
        });
      const storage = yield* make();
      yield* storage.put({
        key: "folder/a ?%",
        body: new TextEncoder().encode("hello"),
        customMetadata: { tenant: "a" },
      });
      const head = yield* storage.head({ key: "folder/a ?%" });
      assert.strictEqual(head.size, 5);
      assert.strictEqual(head.customMetadata?.tenant, "a");
      const result = yield* storage.get({ ref: head.ref, range: { _tag: "Suffix", length: 2 } });
      assert.strictEqual(result._tag, "Object");
      if (result._tag === "Object") {
        assert.strictEqual(result.metadata.size, 5);
        assert.strictEqual(result.range?.start, 3);
        assert.deepStrictEqual(
          Array.from((yield* Stream.runCollect(result.body)).flatMap((chunk) => Array.from(chunk))),
          [108, 111],
        );
      }
      const unchanged = yield* storage.get({
        ref: head.ref,
        condition: { _tag: "IfNoneMatch", etag: head.etag ?? "missing-validator" },
      });
      assert.strictEqual(unchanged._tag, "NotModified");
      const cases = yield* runStorageConformance(() =>
        Effect.gen(function* () {
          fixture.objects.clear();
          return yield* make();
        }),
      );
      assert.isTrue(cases.some((entry) => entry.status === "passed"));
    }),
  ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect("refreshes user credentials per operation without privileged fallback", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      let token = "user-a";
      const storage = yield* SupabaseStorage.make({
        url: fixture.url,
        bucket: "bucket",
        storeId: "test",
        auth: { mode: "user", token: Effect.sync(() => Redacted.make(token)) },
      });
      yield* storage.put({ key: "a", body: new Uint8Array() });
      token = "denied";
      const result = yield* Storage.exists({ key: "a" }).pipe(
        Effect.provideService(Storage, storage),
        Effect.result,
      );
      assert.strictEqual(result._tag === "Failure" && result.failure.reason._tag, "AccessDenied");
      assert.strictEqual(fixture.seen.at(-1)?.token, "Bearer denied");
      assert.isFalse(JSON.stringify(result).includes("RLS denied secret"));
    }),
  ).pipe(Effect.provide(FetchHttpClient.layer)),
);

it.effect("issues protected native grants and rejects unsupported signing methods", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const provider = SupabaseStorage.layer({
        url: fixture.url,
        bucket: "bucket",
        storeId: "test",
        auth: { mode: "service", token: Redacted.make("service") },
      });
      const layer = SupabaseSigning.layer().pipe(Layer.provide(provider));
      yield* Effect.gen(function* () {
        const signing = yield* StorageSigning;
        const signed = yield* signing.sign({
          ref: { storeId: "test", key: "a" },
          method: "GET",
          expiresInSeconds: 60,
        });
        assert.isTrue(Redacted.value(signed.url).includes("download-secret"));
        assert.isFalse(JSON.stringify(signed).includes("download-secret"));
        const upload = yield* (yield* SupabaseUploadGrants).create({ key: "a", upsert: false });
        assert.strictEqual(upload.method, "PUT");
        assert.strictEqual(upload.constraints[0]?.enforcement, "provider");
        const unsupported = yield* signing
          .sign({ ref: { storeId: "test", key: "a" }, method: "DELETE", expiresInSeconds: 60 })
          .pipe(Effect.result);
        assert.strictEqual(
          unsupported._tag === "Failure" && unsupported.failure.reason._tag,
          "Unsupported",
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("validates native metadata without leaking malformed provider content", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      fixture.objects.set("bad", {
        body: Buffer.from([1]),
        etag: '"bad"',
        contentType: "application/octet-stream",
        metadata: { invalid: "private\nprovider-value" },
      });
      const storage = yield* SupabaseStorage.make({
        url: fixture.url,
        bucket: "bucket",
        storeId: "test",
        auth: { mode: "user", token: Redacted.make("user") },
      });
      const result = yield* storage.head({ key: "bad" }).pipe(Effect.result);
      assert.strictEqual(
        result._tag === "Failure" && result.failure.reason._tag,
        "ProviderFailure",
      );
      assert.isFalse(JSON.stringify(result).includes("provider-value"));
    }),
  ).pipe(Effect.provide(FetchHttpClient.layer)),
);
