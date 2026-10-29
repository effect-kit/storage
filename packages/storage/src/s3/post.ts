import type { DateTime } from "effect";
import { Context, Effect, Layer, Redacted, Schema } from "effect";

import { createPresignedPost } from "@aws-sdk/s3-presigned-post";

import type { GrantConstraint } from "../capabilities.js";
import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import type { ObjectRef } from "../object.js";
import { HeaderValue, ObjectKey } from "../object.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { signingCredentials } from "./signing-credentials.js";
import { request } from "./transport.js";

const PostRequest = Schema.Struct({
  key: ObjectKey,
  expiresInSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 604800 })),
  maxBytes: Schema.Natural,
  minBytes: Schema.optional(Schema.Natural),
  contentType: Schema.optional(HeaderValue),
  customMetadata: Schema.optional(
    Schema.Record(Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._-]+$/u)), HeaderValue),
  ),
});
export type S3PostRequest = typeof PostRequest.Type;

export interface S3PostGrant {
  readonly method: "POST";
  readonly ref: ObjectRef;
  readonly url: Redacted.Redacted<string>;
  readonly fields: Redacted.Redacted<Readonly<Record<string, string>>>;
  readonly expiresAt: DateTime.Utc;
  readonly constraints: ReadonlyArray<GrantConstraint>;
}

export class S3Post extends Context.Service<
  S3Post,
  { readonly create: (input: S3PostRequest) => Effect.Effect<S3PostGrant, StorageError> }
>()("@effect-kit/storage/s3/S3Post") {
  static make(storage: S3StorageService): S3Post["Service"] {
    return {
      create: Effect.fnUntraced(function* (input) {
        const value = yield* decodeRequest(PostRequest, input, "post", "s3");
        if ((value.minBytes ?? 0) > value.maxBytes || value.key.includes(`\${filename}`))
          return yield* storageError("post", "s3", { _tag: "InvalidInput", field: "policy" });
        if (
          storage.config.encryption ||
          storage.config.expectedBucketOwner ||
          storage.config.requestPayer
        )
          return yield* storageError("post", "s3", {
            _tag: "Unsupported",
            capability: "post-config-authority",
          });
        const credentials = yield* signingCredentials(storage, value.expiresInSeconds);
        const fields: Record<string, string> = {};
        if (value.contentType !== undefined) fields["Content-Type"] = value.contentType;
        for (const [name, content] of Object.entries(value.customMetadata ?? {}))
          fields[`x-amz-meta-${name.toLowerCase()}`] = content;
        const grant = yield* request(storage, "post", false, () =>
          createPresignedPost(credentials.client, {
            Bucket: storage.config.bucket,
            Key: value.key,
            Expires: credentials.seconds,
            Fields: fields,
            Conditions: [["content-length-range", value.minBytes ?? 0, value.maxBytes]],
          }),
        );
        const policy = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(
            Schema.Struct({
              expiration: Schema.DateTimeUtcFromString,
              conditions: Schema.Array(Schema.Unknown),
            }),
          ),
        )(Buffer.from(grant.fields.Policy ?? "", "base64").toString("utf8")).pipe(
          Effect.mapError(() =>
            storageError("post", "s3", { _tag: "ProviderFailure", code: "InvalidPostPolicy" }),
          ),
        );
        return {
          method: "POST",
          ref: { storeId: storage.storeId, key: value.key },
          url: Redacted.make(grant.url),
          fields: Redacted.make(grant.fields),
          expiresAt: policy.expiration,
          constraints: [
            { kind: "minimum-bytes", enforcement: "provider", value: value.minBytes ?? 0 },
            { kind: "maximum-bytes", enforcement: "provider", value: value.maxBytes },
            ...Object.keys(fields).map((name) => ({
              kind: `field:${name}`,
              enforcement: "provider" as const,
              value: "required",
            })),
          ],
        };
      }),
    };
  }

  static readonly layer = Layer.effect(S3Post, Effect.map(S3Storage, S3Post.make));
}
