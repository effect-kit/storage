import { Context, Effect, Layer } from "effect";

import { CopyObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

import { storageError } from "../../core/errors.js";
import type { ObjectHeaders } from "../../core/object.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { MetadataMerge, MetadataReplace, StorageMetadata } from "../../lifecycle/index.js";
import type { MetadataService } from "../../lifecycle/index.js";
import { mergeHeaders, replaceHeaders } from "../../lifecycle/metadata.js";
import { authority, readEncryption } from "./config.js";
import { S3Storage } from "./index.js";
import type { S3StorageService } from "./index.js";
import { lifecycleTarget } from "./lifecycle-validation.js";
import { condition, encodeCopySource, headers, metadata } from "./object-mapping.js";
import { request } from "./transport.js";
import { validateHeaders } from "./validation.js";

export class S3Metadata extends Context.Service<S3Metadata, MetadataService>()(
  "@effect-kit/storage/s3/S3Metadata",
) {
  static make(storage: S3StorageService): MetadataService {
    const update = Effect.fnUntraced(function* (
      input: typeof MetadataReplace.Type,
      patch?: (typeof MetadataMerge.Type)["patch"],
    ) {
      if (storage.profile)
        return yield* storageError(
          "updateMetadata",
          storage.provider,
          { _tag: "Unsupported", capability: "qualified-conditional-metadata-copy" },
          "not-started",
        );
      if (!input.condition || input.condition._tag !== "IfMatch")
        return yield* storageError(
          "updateMetadata",
          storage.provider,
          { _tag: "Unsupported", capability: "metadata-update-requires-current-if-match" },
          "not-started",
        );
      const target = yield* lifecycleTarget(storage, input.ref, "updateMetadata");
      const before = yield* request(storage, "updateMetadata.head", false, (signal) =>
        storage.client.send(
          new HeadObjectCommand({ ...target, ...readEncryption(storage.config.encryption) }),
          { abortSignal: signal },
        ),
      );
      if (!before.ETag || before.ETag !== input.condition.etag)
        return yield* storageError(
          "updateMetadata",
          storage.provider,
          { _tag: "PreconditionFailed" },
          "not-started",
        );
      if ((before.ContentLength ?? Number.MAX_SAFE_INTEGER) > 5_000_000_000)
        return yield* storageError(
          "updateMetadata",
          storage.provider,
          { _tag: "Unsupported", capability: "multipart-metadata-copy" },
          "not-started",
        );
      const original = metadata(storage, input.ref.key, before);
      const replacement: ObjectHeaders = patch
        ? mergeHeaders(original, patch)
        : replaceHeaders(original, input.headers);
      yield* validateHeaders(replacement, "updateMetadata");
      const customer = readEncryption(storage.config.encryption);
      const output = yield* request(storage, "updateMetadata", true, (signal) =>
        storage.client.send(
          new CopyObjectCommand({
            ...authority(storage.config),
            Key: input.ref.key,
            CopySource: encodeCopySource(storage.config.bucket, input.ref),
            CopySourceIfMatch: before.ETag,
            ...condition(input.condition),
            ...headers(replacement),
            MetadataDirective: "REPLACE",
            TaggingDirective: "COPY",
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
            StorageClass: before.StorageClass,
          }),
          { abortSignal: signal },
        ),
      );
      if (!output.CopyObjectResult?.ETag)
        return yield* storageError(
          "updateMetadata",
          storage.provider,
          { _tag: "ProviderFailure", code: "MissingCopyReceipt" },
          "unknown",
        );
      return metadata(storage, input.ref.key, {
        ...output,
        ...output.CopyObjectResult,
        ...headers(replacement),
        ContentLength: before.ContentLength,
      });
    });
    return {
      replace: (input) =>
        decodeRequest(MetadataReplace, input, "updateMetadata", storage.provider, true).pipe(
          Effect.flatMap((value) => update(value)),
        ),
      merge: (input) =>
        decodeRequest(MetadataMerge, input, "updateMetadata", storage.provider, true).pipe(
          Effect.flatMap((value) =>
            update({ ref: value.ref, headers: {}, condition: value.condition }, value.patch),
          ),
        ),
    };
  }
  static readonly layer = Layer.effectContext(
    Effect.map(S3Storage, (storage) => {
      const service = S3Metadata.make(storage);
      return Context.make(S3Metadata, service).pipe(Context.add(StorageMetadata, service));
    }),
  );
}
