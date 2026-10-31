import { DateTime, Effect, Layer, Redacted, Schema } from "effect";

import { UploadGrantDriver } from "../../uploads/driver.js";
import { failUpload } from "../../uploads/schema.js";
import type { UploadRecord } from "../../uploads/schema.js";
import { operationMetadata } from "../../uploads/verification.js";
import { S3Storage } from "./index.js";
import { S3Signing } from "./signing.js";

const Remote = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("single") }),
  Schema.Struct({ kind: Schema.Literal("post") }),
  Schema.Struct({
    kind: Schema.Literal("multipart"),
    uploadId: Schema.String,
    partBytes: Schema.Natural,
  }),
]);
const remote = (record: UploadRecord) =>
  Schema.decodeUnknownEffect(Remote)(
    record.remote ? Redacted.value(record.remote) : undefined,
  ).pipe(Effect.mapError(() => failUpload("Conflict")));

const seconds = (expiresAt: number) =>
  Effect.map(DateTime.now, (now) =>
    Math.max(1, Math.floor((expiresAt - DateTime.toEpochMillis(now)) / 1000)),
  );

export const S3UploadGrants = {
  layer(options: { readonly partBytes?: number; readonly multipartThreshold?: number } = {}) {
    return Layer.effect(
      UploadGrantDriver,
      Effect.gen(function* () {
        const storage = yield* S3Storage;
        const signing = S3Signing.make(storage);
        const partBytes = options.partBytes ?? 8 * 1024 ** 2;
        if (
          !Number.isSafeInteger(partBytes) ||
          partBytes < storage.multipart.limits.minimumPartBytes ||
          partBytes > Math.min(storage.multipart.limits.maximumPartBytes, 64 * 1024 ** 2)
        )
          return yield* failUpload("BadRequest");
        const ref = (record: UploadRecord) => ({
          storeId: storage.storeId,
          key: record.stagingKey,
        });
        const handle = (record: UploadRecord, uploadId: string) => ({
          ref: ref(record),
          uploadId: Redacted.make(uploadId),
          checksumAlgorithm: "SHA256" as const,
          checksumType: "COMPOSITE" as const,
        });
        const inventory = Effect.fnUntraced(function* (record: UploadRecord, uploadId: string) {
          const parts = [];
          let cursor: string | undefined;
          const seen = new Set<string>();
          do {
            const page = yield* storage.multipart.listParts({
              upload: handle(record, uploadId),
              ...(cursor ? { cursor } : {}),
            });
            parts.push(...page.parts);
            cursor = page.cursor;
            if (
              parts.length > storage.multipart.limits.maximumParts ||
              (cursor && seen.has(cursor))
            )
              return yield* failUpload("Conflict");
            if (cursor) seen.add(cursor);
          } while (cursor);
          return parts;
        });
        return UploadGrantDriver.of({
          begin: Effect.fnUntraced(function* (record) {
            if (storage.config.encryption?.mode === "customer")
              return yield* failUpload("Unsupported");
            const strategy = record.input.strategy ?? "auto";
            if (["resumable", "proxy"].includes(strategy)) return yield* failUpload("Unsupported");
            if (strategy === "post") return { kind: "post" };
            if (
              strategy === "single" ||
              (strategy === "auto" &&
                record.input.file.size < (options.multipartThreshold ?? 16 * 1024 ** 2))
            )
              return { kind: "single" };
            if (
              !record.input.file.size ||
              Math.ceil(record.input.file.size / partBytes) >
                storage.multipart.limits.maximumParts ||
              (storage.multipart.limits.completeConditions !== undefined &&
                !storage.multipart.limits.completeConditions.includes("create-only"))
            )
              return yield* failUpload("Unsupported");
            const upload = yield* storage.multipart.initiate({
              key: record.stagingKey,
              checksum: { ChecksumAlgorithm: "SHA256", ChecksumType: "COMPOSITE" },
              headers: {
                contentType: record.input.file.contentType,
                customMetadata: operationMetadata(record),
              },
            });
            return { kind: "multipart", uploadId: Redacted.value(upload.uploadId), partBytes };
          }),
          grant: Effect.fnUntraced(function* (record, expiresAt) {
            const state = yield* remote(record);
            const expiresInSeconds = yield* seconds(expiresAt);
            if (state.kind === "multipart")
              return {
                _tag: "Multipart",
                checksum: "sha256",
                partBytes: state.partBytes,
                completed: (yield* inventory(record, state.uploadId)).map(
                  ({ partNumber, etag }) => ({ partNumber, etag }),
                ),
                expiresAt,
              };
            if (state.kind === "post") {
              const { S3Post } = yield* Effect.tryPromise({
                try: () => import("./post.js"),
                catch: () => failUpload("Unsupported"),
              });
              const policy = yield* S3Post.make(storage).create({
                key: record.stagingKey,
                minBytes: record.input.file.size,
                maxBytes: record.input.file.size,
                expiresInSeconds,
                contentType: record.input.file.contentType,
              });
              return {
                _tag: "PostForm",
                constraints: policy.constraints,
                url: Redacted.value(policy.url),
                fields: Redacted.value(policy.fields),
                expiresAt: DateTime.toEpochMillis(policy.expiresAt),
              };
            }
            const signed = yield* signing.sign({
              ref: ref(record),
              method: "PUT",
              expiresInSeconds,
              contentLength: record.input.file.size,
              headers: {
                contentType: record.input.file.contentType,
                customMetadata: operationMetadata(record),
              },
              condition: { _tag: "CreateOnly" },
            });
            return {
              _tag: "SignedRequest",
              constraints: signed.constraints,
              method: "PUT",
              url: Redacted.value(signed.url),
              headers: Redacted.value(signed.headers),
              expiresAt: DateTime.toEpochMillis(signed.expiresAt),
            };
          }),
          part: Effect.fnUntraced(function* (record, partNumber, expiresAt, checksum) {
            const state = yield* remote(record);
            if (
              state.kind !== "multipart" ||
              partNumber > Math.ceil(record.input.file.size / state.partBytes)
            )
              return yield* failUpload("BadRequest");
            if (!checksum || !/^[a-f0-9]{64}$/u.test(checksum))
              return yield* failUpload("BadRequest");
            const signed = yield* signing.signPart({
              checksum: { ChecksumSHA256: Buffer.from(checksum, "hex").toString("base64") },
              upload: handle(record, state.uploadId),
              partNumber,
              expiresInSeconds: yield* seconds(expiresAt),
              contentLength: Math.min(
                state.partBytes,
                record.input.file.size - (partNumber - 1) * state.partBytes,
              ),
            });
            return {
              _tag: "SignedRequest",
              constraints: signed.constraints,
              method: "PUT",
              url: Redacted.value(signed.url),
              headers: Redacted.value(signed.headers),
              expiresAt: DateTime.toEpochMillis(signed.expiresAt),
            };
          }),
          complete: Effect.fnUntraced(function* (record, supplied) {
            const state = yield* remote(record);
            if (state.kind !== "multipart") return yield* failUpload("Conflict");
            const parts = yield* inventory(record, state.uploadId);
            const count = Math.ceil(record.input.file.size / state.partBytes);
            const sorted = parts.toSorted((a, b) => a.partNumber - b.partNumber);
            if (
              sorted.length !== count ||
              supplied.length !== count ||
              new Set(supplied.map((p) => p.partNumber)).size !== count ||
              sorted.some(
                (part, index) =>
                  part.partNumber !== index + 1 ||
                  part.size !==
                    Math.min(state.partBytes, record.input.file.size - index * state.partBytes) ||
                  !supplied.some(
                    (p) => p.partNumber === part.partNumber && p.receipt === part.etag,
                  ),
              )
            )
              return yield* failUpload("VerificationFailed");
            yield* storage.multipart.complete({
              upload: handle(record, state.uploadId),
              parts: sorted,
              condition: { _tag: "CreateOnly" },
            });
          }),
          abort: Effect.fnUntraced(function* (record) {
            if (record.remote === undefined) return;
            const state = yield* remote(record);
            if (state.kind === "multipart")
              yield* storage.multipart.abort(handle(record, state.uploadId));
          }),
        });
      }),
    );
  },
};
