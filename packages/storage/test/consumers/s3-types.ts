import { Context, Effect, Schema, Stream } from "effect";

import type { MultipartUploads, ObjectMetadata, StorageError } from "@effect-kit/storage";
import { S3Storage } from "@effect-kit/storage/s3";
import type { S3StorageService } from "@effect-kit/storage/s3";
import { S3Post } from "@effect-kit/storage/s3/post";
import { S3Signing } from "@effect-kit/storage/s3/signing";
import { s3TransferEndpoint, s3SessionHandles } from "@effect-kit/storage/s3/transfers";
import { UploadSessions } from "@effect-kit/storage/sessions";
import { Transfers } from "@effect-kit/storage/transfers";
import { sha256 } from "@effect-kit/storage/transfers/node";

class Source extends Context.Service<Source, Uint8Array>()("s3-consumer/source") {}
class SourceFailure extends Schema.TaggedError<SourceFailure>()("SourceFailure", {}) {}
declare const storage: S3StorageService;
const body = Stream.unwrap(
  Effect.map(Source, (bytes) =>
    Stream.concat(Stream.succeed(bytes), Stream.fail(new SourceFailure())),
  ),
);
const upload = storage.put({ key: "typed", body, contentLength: 10 });
const preserved: Effect.Effect<ObjectMetadata, StorageError | SourceFailure, Source> = upload;
// @ts-expect-error published source requirements cannot disappear
const missing: Effect.Effect<ObjectMetadata, StorageError | SourceFailure> = upload;
const multipart: MultipartUploads = storage.multipart;
const part = storage.multipart.uploadPart({
  upload: {} as never,
  body,
  contentLength: 10,
  partNumber: 1,
});
// @ts-expect-error multipart source failures cannot disappear
const missingPartError: Effect.Effect<unknown, StorageError, Source> = part;
const base = S3Storage.layer({ bucket: "typed-consumer", region: "us-east-1" });
const transfers = Transfers.make(s3TransferEndpoint(storage), { digest: sha256 });
void transfers;
void [preserved, missing, multipart, missingPartError, base, S3Signing.layer, S3Post.layer];

void UploadSessions.make(
  s3TransferEndpoint(storage),
  { digest: sha256 },
  { configurationId: "consumer", handles: s3SessionHandles },
);
