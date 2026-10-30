import { Context, Effect, Schema, Stream } from "effect";

import type { MultipartUploads, ObjectMetadata, StorageError } from "@effect-kit/storage";
import { S3Storage } from "@effect-kit/storage/s3";
import type { S3StorageService } from "@effect-kit/storage/s3";
import { S3Post } from "@effect-kit/storage/s3/post";
import { S3Signing } from "@effect-kit/storage/s3/signing";
import { UploadSessions } from "@effect-kit/storage/sessions";
import { Transfers } from "@effect-kit/storage/transfers";

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
const transfers = Transfers.make();
void transfers;
void [preserved, missing, multipart, missingPartError, base, S3Signing.layer, S3Post.layer];

void UploadSessions.make({ configurationId: "consumer" });

export { SupabaseS3Storage } from "@effect-kit/storage/supabase/s3";

import { B2Storage } from "@effect-kit/storage/b2";
import { HetznerStorage } from "@effect-kit/storage/hetzner";
import { MinioStorage } from "@effect-kit/storage/minio";
import { R2Storage } from "@effect-kit/storage/r2";
import { ScalewayStorage } from "@effect-kit/storage/scaleway";
import { SpacesStorage } from "@effect-kit/storage/spaces";
import { TigrisStorage } from "@effect-kit/storage/tigris";
import { WasabiStorage } from "@effect-kit/storage/wasabi";

void R2Storage.layer;
void MinioStorage.layer;
void TigrisStorage.layer;
void SpacesStorage.layer;
void B2Storage.layer;
void WasabiStorage.layer;
void ScalewayStorage.layer;
void HetznerStorage.layer;
