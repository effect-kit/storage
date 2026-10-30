import { Effect, Schema } from "effect";

import { CreateBucketCommand, DeleteBucketCommand, ListBucketsCommand } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import type { S3StorageService } from "../s3.js";
import { extendRequest } from "../s3/request-extension.js";
import { request } from "../s3/transport.js";

export const Bucket = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/u));
export const createBucket = Effect.fnUntraced(function* (
  storage: S3StorageService,
  input: { readonly bucket: string; readonly sourceBucket?: string; readonly snapshot?: string },
) {
  const value = yield* decodeRequest(
    Schema.Struct({
      bucket: Bucket,
      sourceBucket: Schema.optional(Bucket),
      snapshot: Schema.optional(Schema.NonEmptyString),
    }),
    input,
    "createBucket",
    "tigris",
    true,
  );
  const command = new CreateBucketCommand({ Bucket: value.bucket });
  extendRequest(command, {
    headers: {
      "X-Amz-Acl": "private",
      "X-Amz-Acl-Public-List-Objects-Enabled": "false",
      ...(value.sourceBucket ? { "X-Tigris-Fork-Source-Bucket": value.sourceBucket } : {}),
      ...(value.snapshot ? { "X-Tigris-Fork-Source-Bucket-Snapshot": value.snapshot } : {}),
    },
  });
  yield* request(storage, "createBucket", true, (signal) =>
    storage.client.send(command, { abortSignal: signal }),
  );
  return { _tag: "Accepted" as const, bucket: value.bucket };
});
export const snapshots = (storage: S3StorageService) => ({
  create: Effect.fnUntraced(function* (input: { readonly bucket: string; readonly name?: string }) {
    const value = yield* decodeRequest(
      Schema.Struct({
        bucket: Bucket,
        name: Schema.optional(Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._-]{1,128}$/u))),
      }),
      input,
      "createSnapshot",
      "tigris",
      true,
    );
    let version: string | undefined;
    const command = new CreateBucketCommand({ Bucket: value.bucket });
    extendRequest(command, {
      headers: { "X-Tigris-Snapshot": value.name ? `true; name=${value.name}` : "true" },
      observeHeaders: (headers) => {
        version = headers["x-tigris-snapshot-version"];
      },
    });
    yield* request(storage, "createSnapshot", true, (signal) =>
      storage.client.send(command, { abortSignal: signal }),
    );
    if (!version)
      return yield* storageError(
        "createSnapshot",
        "tigris",
        { _tag: "ProviderFailure", code: "MissingSnapshotReceipt" },
        "unknown",
      );
    return { snapshotVersion: version };
  }),
  listPage: Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly cursor?: string;
    readonly limit?: number;
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({
        bucket: Bucket,
        cursor: Schema.optional(Schema.NonEmptyString),
        limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
      }),
      input,
      "listSnapshots",
      "tigris",
    );
    const command = new ListBucketsCommand({
      ContinuationToken: value.cursor,
      MaxBuckets: value.limit,
    });
    extendRequest(command, { headers: { "X-Tigris-Snapshot": value.bucket } });
    const response = yield* request(storage, "listSnapshots", false, (signal) =>
      storage.client.send(command, { abortSignal: signal }),
    );
    return {
      snapshots: (response.Buckets ?? []).map((snapshot) => ({
        version: snapshot.Name?.split(";")[0],
        name: snapshot.Name?.split("; name=")[1],
        createdAt: snapshot.CreationDate,
      })),
      cursor: response.ContinuationToken,
    };
  }),
  delete: Effect.fnUntraced(function* (input: {
    readonly bucket: string;
    readonly version: string;
    readonly intent: "delete-snapshot";
  }) {
    const value = yield* decodeRequest(
      Schema.Struct({
        bucket: Bucket,
        version: Schema.NonEmptyString,
        intent: Schema.Literal("delete-snapshot"),
      }),
      input,
      "deleteSnapshot",
      "tigris",
      true,
    );
    const command = new DeleteBucketCommand({ Bucket: value.bucket });
    extendRequest(command, { headers: { "X-Tigris-Snapshot-Version": value.version } });
    yield* request(storage, "deleteSnapshot", true, (signal) =>
      storage.client.send(command, { abortSignal: signal }),
    );
  }),
});
