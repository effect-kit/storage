import { Clock, Effect, Layer, Redacted, Schema } from "effect";

import { GcsStorage } from "../gcs.js";
import { UploadGrantDriver } from "../uploads/driver.js";
import { failUpload } from "../uploads/schema.js";
import { operationMetadata } from "../uploads/verification.js";
const decode = (remote: unknown) =>
  Schema.decodeUnknownEffect(
    Schema.Struct({ url: Schema.String, total: Schema.Natural, expiresAt: Schema.Number }),
  )(remote).pipe(Effect.mapError(() => failUpload("Conflict")));

export const GcsUploadGrants = {
  layer(options: { readonly chunkBytes?: number } = {}) {
    return Layer.effect(
      UploadGrantDriver,
      Effect.gen(function* () {
        const storage = yield* GcsStorage;
        const chunkBytes = options.chunkBytes ?? 8 * 1024 ** 2;
        if (chunkBytes < 256 * 1024 || chunkBytes % (256 * 1024) || chunkBytes > 64 * 1024 ** 2)
          return yield* failUpload("BadRequest");
        return UploadGrantDriver.of({
          maximumGrantLifetimeMillis: 7 * 86400000 + 60000,
          begin: Effect.fnUntraced(function* (record) {
            if (
              storage.config.customerKey ||
              !["auto", "resumable"].includes(record.input.strategy ?? "auto")
            )
              return yield* failUpload("Unsupported");
            const upload = yield* storage.offsets.create({
              key: record.stagingKey,
              contentLength: record.input.file.size,
              contentType: record.input.file.contentType,
              customMetadata: operationMetadata(record),
              condition: { _tag: "CreateOnly" },
            });
            const handle = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(Schema.Struct({ url: Schema.String, total: Schema.Natural })),
            )(Redacted.value(upload.handle)).pipe(Effect.mapError(() => failUpload("Unavailable")));
            return { ...handle, expiresAt: (yield* Clock.currentTimeMillis) + 7 * 86400000 };
          }),
          grant: Effect.fnUntraced(function* (record, expiresAt) {
            const remote = yield* decode(record.remote ? Redacted.value(record.remote) : undefined);
            const state = yield* storage.offsets.query({
              protocol: "gcs-resumable-v1",
              ref: { storeId: storage.storeId, key: record.stagingKey },
              handle: Redacted.make(JSON.stringify(remote)),
            });
            if (state._tag === "Expired") return yield* failUpload("Expired");
            if (state._tag === "Unknown") return yield* failUpload("Unavailable");
            return {
              _tag: "Resumable",
              protocol: "gcs",
              url: remote.url,
              headers: {},
              offset: state._tag === "Completed" ? record.input.file.size : state.committedOffset,
              chunkBytes,
              expiresAt: Math.min(expiresAt, remote.expiresAt),
            };
          }),
          abort: Effect.fnUntraced(function* (record) {
            if (record.remote === undefined) return;
            const remote = yield* decode(record.remote ? Redacted.value(record.remote) : undefined);
            yield* storage.offsets.cancel({
              protocol: "gcs-resumable-v1",
              ref: { storeId: storage.storeId, key: record.stagingKey },
              handle: Redacted.make(JSON.stringify(remote)),
            });
          }),
        });
      }),
    );
  },
};
