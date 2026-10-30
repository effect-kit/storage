import { Context, Effect, Layer, Schema } from "effect";

import { CopyObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { VersionRef } from "../lifecycle.js";
import { S3Storage } from "../s3.js";
import type { S3StorageService } from "../s3.js";
import { readEncryption } from "./config.js";
import { lifecycleTarget } from "./lifecycle-validation.js";
import { encodeCopySource, metadata } from "./object-mapping.js";
import { request } from "./transport.js";

const Transition = Schema.Struct({
  ref: VersionRef,
  ifMatch: Schema.NonEmptyString,
  storageClass: Schema.Literals([
    "STANDARD",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "GLACIER_IR",
    "DEEP_ARCHIVE",
  ]),
  intent: Schema.Literal("copy-storage-tier"),
});
const make = (storage: S3StorageService) => ({
  inspectEncryption: Effect.fnUntraced(function* (input: { readonly ref: VersionRef }) {
    const value = yield* decodeRequest(
      Schema.Struct({ ref: VersionRef }),
      input,
      "inspectEncryption",
      storage.provider,
    );
    const target = yield* lifecycleTarget(storage, value.ref, "inspectEncryption", true);
    const observed = yield* request(storage, "inspectEncryption", false, (signal) =>
      storage.client.send(
        new HeadObjectCommand({ ...target, ...readEncryption(storage.config.encryption) }),
        { abortSignal: signal },
      ),
    );
    return {
      algorithm: observed.ServerSideEncryption,
      kmsKeyId: observed.SSEKMSKeyId,
      bucketKeyEnabled: observed.BucketKeyEnabled,
      customerAlgorithm: observed.SSECustomerAlgorithm,
      customerKeyMD5: observed.SSECustomerKeyMD5,
    };
  }),
  transition: Effect.fnUntraced(function* (input: typeof Transition.Type) {
    const value = yield* decodeRequest(Transition, input, "transitionTier", storage.provider, true);
    if (storage.profile)
      return yield* storageError(
        "transitionTier",
        storage.provider,
        { _tag: "Unsupported", capability: "conditional-tier-copy" },
        "not-started",
      );
    const target = yield* lifecycleTarget(storage, value.ref, "transitionTier", true);
    const customer = readEncryption(storage.config.encryption);
    const before = yield* request(storage, "transitionTier.head", false, (signal) =>
      storage.client.send(new HeadObjectCommand({ ...target, ...customer }), {
        abortSignal: signal,
      }),
    );
    if (!before.ETag || before.ETag !== value.ifMatch)
      return yield* storageError(
        "transitionTier",
        storage.provider,
        { _tag: "PreconditionFailed" },
        "not-started",
      );
    if ((before.ContentLength ?? Number.MAX_SAFE_INTEGER) > 5_000_000_000)
      return yield* storageError(
        "transitionTier",
        storage.provider,
        { _tag: "Unsupported", capability: "multipart-tier-copy" },
        "not-started",
      );
    const output = yield* request(storage, "transitionTier", true, (signal) =>
      storage.client.send(
        new CopyObjectCommand({
          Bucket: target.Bucket,
          Key: target.Key,
          CopySource: encodeCopySource(target.Bucket, value.ref),
          CopySourceIfMatch: value.ifMatch,
          IfMatch: value.ifMatch,
          MetadataDirective: "COPY",
          TaggingDirective: "COPY",
          StorageClass: value.storageClass,
          ServerSideEncryption: before.ServerSideEncryption,
          SSEKMSKeyId: before.SSEKMSKeyId,
          BucketKeyEnabled: before.BucketKeyEnabled,
          ...customer,
          ...(storage.config.encryption?.mode === "customer"
            ? {
                CopySourceSSECustomerAlgorithm: customer.SSECustomerAlgorithm,
                CopySourceSSECustomerKey: customer.SSECustomerKey,
                CopySourceSSECustomerKeyMD5: customer.SSECustomerKeyMD5,
              }
            : {}),
          ObjectLockMode: before.ObjectLockMode,
          ObjectLockRetainUntilDate: before.ObjectLockRetainUntilDate,
          ObjectLockLegalHoldStatus: before.ObjectLockLegalHoldStatus,
        }),
        { abortSignal: signal },
      ),
    );
    if (!output.CopyObjectResult?.ETag)
      return yield* storageError(
        "transitionTier",
        storage.provider,
        { _tag: "ProviderFailure", code: "MissingCopyReceipt" },
        "unknown",
      );
    return metadata(storage.config, value.ref.key, {
      ...before,
      ...output,
      ...output.CopyObjectResult,
      VersionId: output.VersionId,
    });
  }),
});
export class S3StorageTier extends Context.Service<S3StorageTier, ReturnType<typeof make>>()(
  "@effect-kit/storage/s3/S3StorageTier",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(S3StorageTier, Effect.map(S3Storage, make));
}
