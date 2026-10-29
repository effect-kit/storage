import { randomUUID } from "node:crypto";

import { Effect, Schema } from "effect";

import {
  AbortMultipartUploadCommand,
  DeleteObjectsCommand,
  ListMultipartUploadsCommand,
  ListObjectVersionsCommand,
} from "@aws-sdk/client-s3";

import type { ObjectMetadata } from "../../src/object.js";
import type { ObjectTarget } from "../../src/requests.js";
import { S3Storage } from "../../src/s3.js";
import type { S3StorageService } from "../../src/s3.js";
import { mapS3Error } from "../../src/s3/transport.js";
import { Storage } from "../../src/storage.js";

const Environment = Schema.Struct({
  mode: Schema.Literals(["local", "aws"]),
  bucket: Schema.NonEmptyString,
  region: Schema.NonEmptyString,
  prefix: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9/_-]*\/$/u)),
  endpoint: Schema.optional(Schema.String),
});

export const environment = Schema.decodeUnknownSync(Environment)({
  mode: process.env.STORAGE_S3_ACCEPTANCE,
  bucket: process.env.STORAGE_S3_TEST_BUCKET,
  region: process.env.STORAGE_S3_TEST_REGION,
  prefix: process.env.STORAGE_S3_TEST_PREFIX,
  ...(process.env.STORAGE_S3_TEST_ENDPOINT
    ? { endpoint: process.env.STORAGE_S3_TEST_ENDPOINT }
    : {}),
});
if (environment.mode === "aws" && environment.endpoint)
  throw new Error("AWS acceptance cannot use an endpoint override");

const native = <A>(execute: () => Promise<A>) =>
  Effect.tryPromise({ try: execute, catch: (cause) => mapS3Error(cause, "fixture-cleanup", true) });

const cleanup = Effect.fnUntraced(function* (storage: S3StorageService, prefix: string) {
  const Bucket = storage.config.bucket;
  let key: string | undefined;
  let upload: string | undefined;
  do {
    const page = yield* native(() =>
      storage.client.send(
        new ListMultipartUploadsCommand({
          Bucket,
          Prefix: prefix,
          KeyMarker: key,
          UploadIdMarker: upload,
        }),
      ),
    );
    for (const pending of page.Uploads ?? []) {
      if (!pending.Key?.startsWith(prefix) || !pending.UploadId)
        return yield* Effect.die(new Error("Fixture upload escaped its owned prefix"));
      yield* native(() =>
        storage.client.send(
          new AbortMultipartUploadCommand({ Bucket, Key: pending.Key, UploadId: pending.UploadId }),
        ),
      );
    }
    key = page.IsTruncated ? page.NextKeyMarker : undefined;
    upload = page.IsTruncated ? page.NextUploadIdMarker : undefined;
    if (page.IsTruncated && !key)
      return yield* Effect.die(new Error("Fixture upload cleanup has no continuation"));
  } while (key !== undefined);

  key = undefined;
  let version: string | undefined;
  do {
    const page = yield* native(() =>
      storage.client.send(
        new ListObjectVersionsCommand({
          Bucket,
          Prefix: prefix,
          KeyMarker: key,
          VersionIdMarker: version,
        }),
      ),
    );
    const objects = [...(page.Versions ?? []), ...(page.DeleteMarkers ?? [])].map((entry) => ({
      Key: entry.Key,
      VersionId: entry.VersionId,
    }));
    if (objects.some((entry) => !entry.Key?.startsWith(prefix)))
      return yield* Effect.die(new Error("Fixture object escaped its owned prefix"));
    for (let offset = 0; offset < objects.length; offset += 1000) {
      const deleted = yield* native(() =>
        storage.client.send(
          new DeleteObjectsCommand({
            Bucket,
            Delete: { Objects: objects.slice(offset, offset + 1000), Quiet: false },
          }),
        ),
      );
      if (deleted.Errors?.length)
        return yield* Effect.die(new Error("Fixture cleanup has per-key failures"));
    }
    key = page.IsTruncated ? page.NextKeyMarker : undefined;
    version = page.IsTruncated ? page.NextVersionIdMarker : undefined;
    if (page.IsTruncated && !key)
      return yield* Effect.die(new Error("Fixture object cleanup has no continuation"));
  } while (key !== undefined);
});

export const s3Fixture = Effect.fnUntraced(function* () {
  const storage = yield* S3Storage.make({
    bucket: environment.bucket,
    region: environment.region,
    ...(environment.endpoint ? { endpoint: environment.endpoint, forcePathStyle: true } : {}),
    requestTimeoutMillis: 15_000,
  });
  const prefix = `${environment.prefix}${randomUUID()}/`;
  yield* Effect.addFinalizer(() => cleanup(storage, prefix).pipe(Effect.orDie));
  return { storage, prefix };
});

/** Fixture-only namespace mapping lets the public suite use its own logical keys. */
export const conformanceFixture = Effect.fnUntraced(function* () {
  const { storage, prefix } = yield* s3Fixture();
  const storeId = `${storage.storeId}:${prefix}`;
  const observed = (input: ObjectMetadata): ObjectMetadata => ({
    ...input,
    ref: { ...input.ref, storeId, key: input.ref.key.slice(prefix.length) },
  });
  const selected = (input: ObjectTarget): ObjectTarget =>
    "ref" in input
      ? { ref: { ...input.ref, storeId: storage.storeId, key: `${prefix}${input.ref.key}` } }
      : { key: `${prefix}${input.key}` };
  return Storage.make({
    ...storage,
    storeId,
    put: (input) =>
      storage.put({ ...input, key: `${prefix}${input.key}` }).pipe(Effect.map(observed)),
    head: (input) => storage.head(selected(input)).pipe(Effect.map(observed)),
    get: (input) =>
      storage
        .get({ ...input, ...selected(input) })
        .pipe(Effect.map((response) => ({ ...response, metadata: observed(response.metadata) }))),
    delete: (input) => storage.delete({ ...selected(input), condition: input.condition }),
    listPage: (input = {}) =>
      storage.listPage({ ...input, prefix: `${prefix}${input.prefix ?? ""}` }).pipe(
        Effect.map((page) => ({
          cursor: page.cursor,
          entries: page.entries.map((entry) =>
            entry._tag === "Object"
              ? { _tag: "Object" as const, metadata: observed(entry.metadata) }
              : { _tag: "CommonPrefix" as const, prefix: entry.prefix.slice(prefix.length) },
          ),
        })),
      ),
  });
});
