import { Context, Effect, Layer, Schema } from "effect";

import { GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { XMLBuilder } from "fast-xml-parser";

import { storageError } from "../../core/errors.js";
import { decodeResponse } from "../../internal/provider-response.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { S3Storage } from "../s3/index.js";
import type { S3StorageService } from "../s3/index.js";
import { extendRequest } from "../s3/request-extension.js";
import { request } from "../s3/transport.js";
import { readXml } from "./xml.js";

const Policy = Schema.Struct({
  status: Schema.Literals(["enabled", "disabled"]),
  retentionDays: Schema.Natural,
  conditionalHold: Schema.Boolean,
  deleteAfterRetention: Schema.Boolean,
  lockTime: Schema.String.check(
    Schema.makeFilter(
      (value) => value === "off" || value === "now" || Number.isFinite(Date.parse(value)),
    ),
  ),
});
const make = (storage: S3StorageService) => {
  const guard = () =>
    storage.provider === "wasabi"
      ? Effect.void
      : Effect.fail(
          storageError("compliance", "wasabi", { _tag: "InvalidConfiguration", field: "provider" }),
        );
  return {
    inspect: Effect.fnUntraced(function* () {
      yield* guard();
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const command = new GetObjectCommand({
            Bucket: storage.config.bucket,
            Key: "compliance",
          });
          extendRequest(command, { bucketRoot: true, query: { compliance: "" } });
          const response = yield* request(storage, "getLegacyCompliance", false, (signal) =>
            storage.client.send(command, { abortSignal: signal }),
          );
          const receipt = yield* readXml(response.Body, "getLegacyCompliance");
          return yield* decodeResponse(
            Schema.Struct({
              BucketComplianceConfiguration: Schema.Struct({
                Status: Schema.Literals(["enabled", "disabled"]),
                LockTime: Schema.optional(Schema.String),
                IsLocked: Schema.optional(Schema.Literals(["true", "false"])),
                RetentionDays: Schema.String,
                ConditionalHold: Schema.optional(Schema.Literals(["true", "false"])),
                DeleteAfterRetention: Schema.optional(Schema.Literals(["true", "false"])),
              }),
            }),
            receipt.decoded,
            "getLegacyCompliance",
            "wasabi",
          ).pipe(Effect.map((value) => value.BucketComplianceConfiguration));
        }),
      );
    }),
    set: Effect.fnUntraced(function* (input: {
      readonly policy: typeof Policy.Type;
      readonly intent: "change-legacy-compliance";
      readonly lockIntent?: "irreversibly-lock-compliance";
    }) {
      const value = yield* decodeRequest(
        Schema.Struct({
          policy: Policy,
          intent: Schema.Literal("change-legacy-compliance"),
          lockIntent: Schema.optional(Schema.Literal("irreversibly-lock-compliance")),
        }),
        input,
        "setLegacyCompliance",
        "wasabi",
        true,
      );
      yield* guard();
      if (value.policy.lockTime !== "off" && !value.lockIntent)
        return yield* storageError(
          "setLegacyCompliance",
          "wasabi",
          { _tag: "InvalidInput", field: "lockIntent" },
          "not-started",
        );
      const body = new XMLBuilder({}).build({
        BucketComplianceConfiguration: {
          Status: value.policy.status,
          LockTime: value.policy.lockTime,
          RetentionDays: value.policy.retentionDays,
          ConditionalHold: value.policy.conditionalHold,
          DeleteAfterRetention: value.policy.deleteAfterRetention,
        },
      }) as string;
      const command = new PutObjectCommand({
        Bucket: storage.config.bucket,
        Key: "compliance",
        Body: body,
        ContentType: "application/xml",
      });
      extendRequest(command, { bucketRoot: true, query: { compliance: "" } });
      yield* request(storage, "setLegacyCompliance", true, (signal) =>
        storage.client.send(command, { abortSignal: signal }),
      );
      return { _tag: "Accepted" as const };
    }),
  };
};
/** Wasabi legacy bucket Compliance is separate from S3 Object Lock. */
export class WasabiCompliance extends Context.Service<WasabiCompliance, ReturnType<typeof make>>()(
  "@effect-kit/storage/wasabi/WasabiCompliance",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(WasabiCompliance, Effect.map(S3Storage, make));
}
