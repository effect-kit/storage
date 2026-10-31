import { Effect, Schema } from "effect";

import { decodeRequest } from "../../internal/request-validation.js";
import type { GcsStorageService } from "./index.js";
import { decodeJson, jsonRequest } from "./transport.js";

const Policy = Schema.Struct({
  version: Schema.Literals([1, 3]),
  etag: Schema.NonEmptyString,
  bindings: Schema.Array(
    Schema.Struct({
      role: Schema.NonEmptyString,
      members: Schema.Array(Schema.NonEmptyString),
      condition: Schema.optional(
        Schema.Struct({
          title: Schema.NonEmptyString,
          expression: Schema.NonEmptyString,
          description: Schema.optional(Schema.String),
        }),
      ),
    }),
  ),
});
export const iam = (storage: GcsStorageService) => ({
  get: Effect.fnUntraced(function* (input: { readonly bucket: string }) {
    const value = yield* decodeRequest(
      Schema.Struct({ bucket: Schema.NonEmptyString }),
      input,
      "getIamPolicy",
      "gcs",
    );
    const output = yield* jsonRequest(storage, "getIamPolicy", false, {
      uri: `${storage.config.apiEndpoint}/storage/v1/b/${encodeURIComponent(value.bucket)}/iam`,
      qs: { optionsRequestedPolicyVersion: "3" },
      json: true,
    });
    return yield* decodeJson(Policy, output.body, "getIamPolicy");
  }),
  set: Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly policy: typeof Policy.Type;
    readonly intent: "replace-bucket-iam-policy";
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({
        bucket: Schema.NonEmptyString,
        policy: Policy,
        intent: Schema.Literal("replace-bucket-iam-policy"),
      }),
      input,
      "setIamPolicy",
      "gcs",
      true,
    );
    const output = yield* jsonRequest(storage, "setIamPolicy", true, {
      uri: `${storage.config.apiEndpoint}/storage/v1/b/${encodeURIComponent(value.bucket)}/iam`,
      method: "PUT",
      json: value.policy,
    });
    return yield* decodeJson(Policy, output.body, "setIamPolicy");
  }),
});
