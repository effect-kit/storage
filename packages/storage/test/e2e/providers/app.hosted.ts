/* oxlint-disable no-await-in-loop -- Rejected grants are checked before the valid upload to the same disposable key. */
import { randomUUID } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Context, Effect, Layer, Redacted, Schema } from "effect";

import { StorageSigning } from "../../../src/core/capabilities.js";
import type { StorageError } from "../../../src/core/errors.js";
import { Storage } from "../../../src/core/storage.js";
import { SupabaseStorage } from "../../../src/providers/supabase/index.js";
import { SupabaseSigning, SupabaseUploadGrants } from "../../../src/providers/supabase/signing.js";
import { VercelStorage } from "../../../src/providers/vercel/index.js";
import { VercelSigning, VercelUploadGrants } from "../../../src/providers/vercel/signing.js";
import * as Runtime from "../../../src/runtime/node.js";
import { TransferDriver, Transfers, UploadSource } from "../../../src/transfers/index.js";

const settings = Schema.decodeUnknownSync(
  Schema.Struct({
    provider: Schema.Literals(["supabase", "vercel"]),
    prefix: Schema.String.check(Schema.isPattern(/^effect-storage-acceptance\/[A-Za-z0-9_-]+\/$/u)),
  }),
)({
  provider: process.env.STORAGE_APP_HOSTED_PROVIDER,
  prefix: process.env.STORAGE_APP_TEST_PREFIX,
});
const prefix = `${settings.prefix}${randomUUID()}/`;
function required(name: string) {
  const value = process.env[name];
  if (!value) throw new Error(`Existing hosted test configuration required: ${name}`);
  return value;
}
const supabase = (mode: "user" | "service", token: string, resumable = false) =>
  SupabaseStorage.layer({
    url: required("STORAGE_SUPABASE_URL"),
    bucket: required("STORAGE_SUPABASE_BUCKET"),
    storeId: `supabase:hosted:${mode}`,
    auth: { mode, token: Redacted.make(token) },
    resumable,
  });

it.live("qualifies the explicitly selected hosted namespace without creating infrastructure", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const provider: Layer.Layer<Storage | TransferDriver, StorageError> =
        settings.provider === "supabase"
          ? supabase("user", required("STORAGE_SUPABASE_USER_A_JWT"), true)
          : VercelStorage.layer({
              storeId: required("STORAGE_VERCEL_STORE_ID"),
              access: required("STORAGE_VERCEL_ACCESS") === "public" ? "public" : "private",
              credential: { mode: "token", token: Redacted.make(required("STORAGE_VERCEL_TOKEN")) },
            });
      const context = yield* Layer.build(provider);
      const storage = Context.get(context, Storage);
      const keys = ["primitive", "resumable", "grant"].map((key) => prefix + key);
      yield* Effect.addFinalizer(() =>
        Effect.forEach(keys, (key) => storage.delete({ key }), { discard: true }).pipe(
          Effect.orDie,
        ),
      );
      const key = keys[0] ?? "invalid";
      const receipt = yield* storage.put({
        key,
        body: new Uint8Array([1, 2, 3]),
        condition: { _tag: "CreateOnly" },
      });
      const observed = yield* storage.head({ key });
      assert.strictEqual(observed.size, 3);
      const read = yield* Storage.getBytes({ key, maxBytes: 3 }).pipe(
        Effect.provideService(Storage, storage),
      );
      assert.deepStrictEqual(Array.from(read), [1, 2, 3]);
      const ranged = yield* Storage.getBytes({
        key,
        range: { _tag: "Suffix", length: 2 },
        maxBytes: 3,
      }).pipe(Effect.provideService(Storage, storage));
      assert.deepStrictEqual(Array.from(ranged), [2, 3]);
      if (observed.etag)
        assert.strictEqual(
          (yield* storage.get({ key, condition: { _tag: "IfNoneMatch", etag: observed.etag } }))
            ._tag,
          "NotModified",
        );
      const supabaseSigner = () =>
        SupabaseSigning.layer().pipe(
          Layer.provide(supabase("user", required("STORAGE_SUPABASE_USER_A_JWT"))),
        );
      const vercelSigner = () =>
        VercelSigning.layer().pipe(
          Layer.provide(
            VercelStorage.layer({
              storeId: required("STORAGE_VERCEL_STORE_ID"),
              access: required("STORAGE_VERCEL_ACCESS") === "public" ? "public" : "private",
              credential: { mode: "token", token: Redacted.make(required("STORAGE_VERCEL_TOKEN")) },
            }),
          ),
        );
      const signingRequest = Effect.flatMap(StorageSigning, (signer) =>
        signer.sign({ ref: observed.ref, method: "GET", expiresInSeconds: 60 }),
      );
      const signed = yield* settings.provider === "supabase"
        ? signingRequest.pipe(Effect.provide(supabaseSigner()))
        : signingRequest.pipe(Effect.provide(vercelSigner()));
      const signedBytes = yield* Effect.tryPromise({
        try: async () => {
          const response = await fetch(Redacted.value(signed.url), {
            headers: Redacted.value(signed.headers),
            signal: AbortSignal.timeout(30000),
          });
          assert.strictEqual(response.status, 200);
          return new Uint8Array(await response.arrayBuffer());
        },
        catch: () => new Error("Hosted signed read did not return expected bytes"),
      });
      assert.deepStrictEqual(Array.from(signedBytes), [1, 2, 3]);
      const grantKey = keys[2] ?? "invalid";
      if (settings.provider === "supabase") {
        const grant = yield* Effect.flatMap(SupabaseUploadGrants, (grants) =>
          grants.create({ key: grantKey }),
        ).pipe(
          Effect.provide(
            SupabaseSigning.layer().pipe(
              Layer.provide(supabase("user", required("STORAGE_SUPABASE_USER_A_JWT"))),
            ),
          ),
        );
        yield* Effect.tryPromise({
          try: async () => {
            const response = await fetch(Redacted.value(grant.url), {
              method: grant.method,
              headers: {
                ...Redacted.value(grant.headers),
                "content-type": "application/octet-stream",
              },
              body: new Uint8Array([7]),
              signal: AbortSignal.timeout(30000),
            });
            await response.body?.cancel();
            assert.isTrue(response.ok);
          },
          catch: () => new Error("Hosted Supabase upload grant failed"),
        });
      } else {
        const grant = yield* Effect.flatMap(VercelUploadGrants, (grants) =>
          grants.create({
            key: grantKey,
            expiresInSeconds: 60,
            maximumSizeInBytes: 2,
            allowedContentTypes: ["application/octet-stream"],
          }),
        ).pipe(Effect.provide(vercelSigner()));
        yield* Effect.tryPromise({
          try: async () => {
            for (const [contentType, body] of [
              ["application/octet-stream", new Uint8Array(3)],
              ["text/plain", new Uint8Array(1)],
            ] as const) {
              const response = await fetch(Redacted.value(grant.url), {
                method: "PUT",
                headers: { "content-type": contentType },
                body,
                signal: AbortSignal.timeout(30000),
              });
              await response.body?.cancel();
              assert.isTrue(response.status >= 400 && response.status < 500);
            }
            const accepted = await fetch(Redacted.value(grant.url), {
              method: "PUT",
              headers: { "content-type": "application/octet-stream" },
              body: new Uint8Array([7]),
              signal: AbortSignal.timeout(30000),
            });
            await accepted.body?.cancel();
            assert.isTrue(accepted.ok);
          },
          catch: () => new Error("Hosted Vercel upload grant enforcement failed"),
        });
      }
      if (settings.provider === "supabase") {
        for (const token of [
          required("STORAGE_SUPABASE_USER_B_JWT"),
          required("STORAGE_SUPABASE_EXPIRED_JWT"),
        ]) {
          const rejected = yield* Storage.getBytes({ key, maxBytes: 3 }).pipe(
            Effect.provide(supabase("user", token)),
            Effect.result,
          );
          assert.strictEqual(rejected._tag, "Failure");
          if (rejected._tag === "Failure")
            assert.isTrue(["AccessDenied", "NotFound"].includes(rejected.failure.reason._tag));
        }
        const privileged = yield* Storage.getBytes({ key, maxBytes: 3 }).pipe(
          Effect.provide(supabase("service", required("STORAGE_SUPABASE_SERVICE_TOKEN"))),
        );
        assert.deepStrictEqual(Array.from(privileged), [1, 2, 3]);
      } else if (receipt.etag) {
        const stale = yield* storage
          .delete({ key, condition: { _tag: "IfMatch", etag: '"deliberately-stale"' } })
          .pipe(Effect.result);
        assert.strictEqual(
          stale._tag === "Failure" && stale.failure.reason._tag,
          "PreconditionFailed",
        );
      }
      const managed = yield* Layer.build(Transfers.layer({ maxBytes: 10 * 1024 ** 2 })).pipe(
        Effect.provide(context.pipe(Context.pick(Storage, TransferDriver))),
      );
      const transfers = Context.get(managed, Transfers);
      const uploaded = yield* transfers.upload({
        key: keys[1] ?? "invalid",
        source: UploadSource.bytes(new Uint8Array(7 * 1024 ** 2).fill(9)),
        strategy: settings.provider === "supabase" ? "offset" : "multipart",
      });
      assert.strictEqual(uploaded.metadata.size, 7 * 1024 ** 2);
    }),
  ).pipe(Effect.provide(Runtime.layer)),
);
