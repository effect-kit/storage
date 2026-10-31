import { Clock, Context, DateTime, Effect, Layer, Redacted, Schema } from "effect";

import type { GrantConstraint } from "../../core/capabilities.js";
import { storageError } from "../../core/errors.js";
import type { StorageError } from "../../core/errors.js";
import { HeaderValue } from "../../core/object.js";
import type { ObjectRef } from "../../core/object.js";
import { GcsStorage } from "./index.js";
import { GcsKey } from "./validation.js";

const PostRequest = Schema.Struct({
  key: GcsKey,
  expiresInSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 604800 })),
  minBytes: Schema.optional(Schema.Natural),
  maxBytes: Schema.Natural,
  contentType: Schema.optional(HeaderValue),
});
export type GcsPostRequest = typeof PostRequest.Type;
export interface GcsPostGrant {
  readonly method: "POST";
  readonly ref: ObjectRef;
  readonly url: Redacted.Redacted<string>;
  readonly fields: Redacted.Redacted<Readonly<Record<string, string>>>;
  readonly expiresAt: DateTime.Utc;
  readonly constraints: ReadonlyArray<GrantConstraint>;
}
export class GcsPost extends Context.Service<
  GcsPost,
  { readonly create: (input: GcsPostRequest) => Effect.Effect<GcsPostGrant, StorageError> }
>()("@effect-kit/storage/gcs/GcsPost") {
  static layer(options: { readonly authorityExpiresAt?: DateTime.Utc } = {}) {
    return Layer.effect(
      GcsPost,
      Effect.gen(function* () {
        const storage = yield* GcsStorage;
        return GcsPost.of({
          create: Effect.fnUntraced(function* (input) {
            const value = yield* Schema.decodeUnknownEffect(PostRequest, {
              onExcessProperty: "error",
            })(input).pipe(
              Effect.mapError(() =>
                storageError(
                  "post",
                  "gcs",
                  { _tag: "InvalidInput", field: "policy" },
                  "not-started",
                ),
              ),
            );
            const now = yield* Clock.currentTimeMillis;
            const expiry =
              Math.floor(
                Math.min(
                  now + value.expiresInSeconds * 1000,
                  options.authorityExpiresAt
                    ? DateTime.toEpochMillis(options.authorityExpiresAt)
                    : Infinity,
                ) / 1000,
              ) * 1000;
            if (
              (value.minBytes ?? 0) > value.maxBytes ||
              value.key.includes(`\${filename}`) ||
              expiry <= now ||
              storage.config.apiEndpoint !== "https://storage.googleapis.com" ||
              storage.config.kmsKeyName ||
              storage.config.customerKey
            )
              return yield* storageError(
                "post",
                "gcs",
                { _tag: "Unsupported", capability: "policy-or-authority" },
                "not-started",
              );
            const [grant] = yield* Effect.tryPromise({
              try: () =>
                storage.client
                  .bucket(storage.config.bucket)
                  .file(value.key)
                  .generateSignedPostPolicyV4({
                    expires: expiry,
                    fields: value.contentType ? { "Content-Type": value.contentType } : {},
                    conditions: [["content-length-range", value.minBytes ?? 0, value.maxBytes]],
                  }),
              catch: () => storageError("post", "gcs", { _tag: "AccessDenied" }, "not-started"),
            });
            return {
              method: "POST" as const,
              ref: { storeId: storage.storeId, key: value.key },
              url: Redacted.make(grant.url),
              fields: Redacted.make(grant.fields),
              expiresAt: DateTime.makeUnsafe(expiry),
              constraints: [
                {
                  kind: "minimum-bytes",
                  enforcement: "provider" as const,
                  value: value.minBytes ?? 0,
                },
                { kind: "maximum-bytes", enforcement: "provider" as const, value: value.maxBytes },
              ],
            };
          }),
        });
      }),
    );
  }
}
