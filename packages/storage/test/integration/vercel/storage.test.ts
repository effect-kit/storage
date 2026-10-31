import { assert, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";

import { StorageSigning } from "../../../src/core/capabilities.js";
import { Storage } from "../../../src/core/storage.js";
import { VercelStorage } from "../../../src/providers/vercel/index.js";
import { VercelSigning, VercelUploadGrants } from "../../../src/providers/vercel/signing.js";
import { httpFixture } from "../../fixtures/vercel/http-fixture.js";

export const config = {
  storeId: "fixture",
  access: "private" as const,
  credential: { mode: "token" as const, token: Redacted.make("vercel_blob_rw_fixture_secret") },
};

it.effect("uses exact paths, same-response metadata, 304, ranges and mutation conditions", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* VercelStorage.make(config);
      const first = yield* storage.put({
        key: "folder/a ?%",
        body: new TextEncoder().encode("hello"),
        contentType: "text/plain",
        condition: { _tag: "CreateOnly" },
      });
      assert.strictEqual(first.ref.key, "folder/a ?%");
      assert.strictEqual(fixture.seen[0]?.headers["x-add-random-suffix"], "0");
      const unchanged = yield* storage.get({
        ref: first.ref,
        condition: { _tag: "IfNoneMatch", etag: first.etag ?? "missing-validator" },
      });
      assert.strictEqual(unchanged._tag, "NotModified");
      const suffix = yield* Storage.getText({
        ref: first.ref,
        range: { _tag: "Suffix", length: 2 },
        maxBytes: 5,
      }).pipe(Effect.provideService(Storage, storage));
      assert.strictEqual(suffix, "lo");
      const stale = yield* storage
        .put({
          key: first.ref.key,
          body: new Uint8Array(),
          condition: { _tag: "IfMatch", etag: '"stale"' },
        })
        .pipe(Effect.result);
      assert.strictEqual(
        stale._tag === "Failure" && stale.failure.reason._tag,
        "PreconditionFailed",
      );
      const rejectedDelete = yield* storage
        .delete({ ref: first.ref, condition: { _tag: "IfMatch", etag: '"stale"' } })
        .pipe(Effect.result);
      assert.strictEqual(
        rejectedDelete._tag === "Failure" && rejectedDelete.failure.reason._tag,
        "PreconditionFailed",
      );
      fixture.cache(Buffer.from("old"));
      assert.strictEqual(
        yield* Storage.getText({ ref: first.ref, maxBytes: 5 }).pipe(
          Effect.provideService(Storage, storage),
        ),
        "hello",
      );
      assert.isTrue(fixture.seen.some((entry) => entry.path.includes("cache=0")));
      yield* storage.delete({
        ref: first.ref,
        condition: { _tag: "IfMatch", etag: first.etag ?? "missing-validator" },
      });
      yield* storage.delete({ ref: first.ref });
    }),
  ),
);

it.effect("keeps one SDK attempt, rejects foreign token stores and refreshes credentials", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      let secret = "vercel_blob_rw_fixture_secret";
      const storage = yield* VercelStorage.make({
        ...config,
        credential: { mode: "token", token: Effect.sync(() => Redacted.make(secret)) },
      });
      yield* storage.put({ key: "a", body: new Uint8Array() });
      secret = "vercel_blob_rw_fixture_denied";
      const denied = yield* storage.get({ key: "a" }).pipe(Effect.result);
      assert.strictEqual(denied._tag === "Failure" && denied.failure.reason._tag, "AccessDenied");
      secret = "vercel_blob_rw_other_secret";
      const before = fixture.seen.length;
      assert.strictEqual((yield* storage.head({ key: "a" }).pipe(Effect.result))._tag, "Failure");
      assert.strictEqual(fixture.seen.length, before);
      secret = "vercel_blob_rw_fixture_secret";
      fixture.fail();
      assert.strictEqual((yield* storage.head({ key: "a" }).pipe(Effect.result))._tag, "Failure");
      assert.strictEqual(fixture.seen.length, before + 1);
    }),
  ),
);

it.live("issues operation-scoped signing and provider-bound upload constraints", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* httpFixture;
      const provider = VercelStorage.layer(config);
      const signing = VercelSigning.layer().pipe(Layer.provide(provider));
      yield* Effect.gen(function* () {
        const signer = yield* StorageSigning;
        for (const method of ["GET", "HEAD", "PUT", "DELETE"] as const) {
          const grant = yield* signer.sign({
            ref: { storeId: "vercel:private:fixture", key: "a" },
            method,
            expiresInSeconds: 60,
          });
          assert.isTrue(Redacted.value(grant.url).includes("vercel-blob"));
          assert.isFalse(JSON.stringify(grant).includes("delegationToken"));
        }
        const grants = yield* VercelUploadGrants;
        for (const input of [
          { key: "a", expiresInSeconds: 60, maximumSizeInBytes: 0 },
          { key: "a", expiresInSeconds: 60, maximumSizeInBytes: 1, allowedContentTypes: [] },
        ]) {
          const rejected = yield* grants.create(input).pipe(Effect.result);
          assert.strictEqual(
            rejected._tag === "Failure" && rejected.failure.reason._tag,
            "InvalidInput",
          );
        }
        const grant = yield* (yield* VercelUploadGrants).create({
          key: "a",
          expiresInSeconds: 60,
          maximumSizeInBytes: 1024,
          allowedContentTypes: ["image/png"],
        });
        assert.strictEqual(
          grant.constraints.find((constraint) => constraint.kind === "maximum-size")?.enforcement,
          "provider",
        );
        assert.strictEqual(
          (yield* signer
            .sign({
              ref: { storeId: "vercel:private:fixture", key: "*" },
              method: "GET",
              expiresInSeconds: 60,
            })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(signing));
    }),
  ),
);

it.effect("refreshes explicit OIDC authority without falling back to a store token", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      let token = "oidc-one";
      const storage = yield* VercelStorage.make({
        ...config,
        credential: { mode: "oidc", token: Effect.sync(() => Redacted.make(token)) },
      });
      yield* storage.put({ key: "oidc", body: new Uint8Array([1]) });
      token = "oidc-two";
      yield* storage.head({ key: "oidc" });
      assert.strictEqual(fixture.seen.at(-1)?.headers.authorization, "Bearer oidc-two");
      assert.strictEqual(fixture.seen.at(-1)?.headers["x-vercel-blob-store-id"], "fixture");
    }),
  ),
);

it.effect("keeps public cache observations distinct from pinned origin identity", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fixture = yield* httpFixture;
      const storage = yield* VercelStorage.make({ ...config, access: "public" });
      const saved = yield* storage.put({ key: "public", body: new TextEncoder().encode("new") });
      fixture.cache(Buffer.from("old"));
      const delivery = yield* Storage.getText({ key: "public", maxBytes: 3 }).pipe(
        Effect.provideService(Storage, storage),
      );
      assert.strictEqual(delivery, "old");
      assert.isFalse(fixture.seen.at(-1)?.path.includes("cache=0"));
      const pinned = yield* storage
        .get({ key: "public", condition: { _tag: "IfMatch", etag: saved.etag ?? "missing" } })
        .pipe(Effect.result);
      assert.strictEqual(
        pinned._tag === "Failure" && pinned.failure.reason._tag,
        "PreconditionFailed",
      );
    }),
  ),
);
