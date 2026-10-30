import { Effect, Stream } from "effect";

import type { ObjectMetadata } from "../object.js";
import { sha256 } from "../runtime/portable-hash.js";
import type { StorageService } from "../storage.js";
import { failUpload } from "./schema.js";
import type { UploadRecord } from "./schema.js";

export const operationMetadata = (record: UploadRecord) => ({ "effect-storage-upload": record.id });
export const digestText = (text: string) => {
  const hash = sha256.create();
  hash.update(new TextEncoder().encode(text));
  return hash.finish().value;
};
export const pinned = (object: ObjectMetadata) => {
  if (!object.ref.version && !object.etag) return Effect.fail(failUpload("Unsupported"));
  return Effect.succeed({
    ref: object.ref,
    ...(object.ref.version
      ? {}
      : { condition: { _tag: "IfMatch" as const, etag: object.etag ?? "" } }),
  });
};
export const verify = Effect.fnUntraced(function* (
  storage: StorageService,
  object: ObjectMetadata,
  record: UploadRecord,
) {
  if (object.size !== record.input.file.size) return yield* failUpload("VerificationFailed");
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const selected = yield* pinned(object);
      const download = yield* storage.get(selected);
      if (download._tag !== "Object") return yield* failUpload("VerificationFailed");
      const hash = sha256.create();
      let bytes = 0;
      yield* Stream.runForEach(download.body, (chunk) =>
        Effect.gen(function* () {
          bytes += chunk.length;
          if (bytes > record.input.file.size) return yield* failUpload("VerificationFailed");
          hash.update(chunk);
        }),
      );
      if (bytes !== record.input.file.size || hash.finish().value !== record.input.file.sha256)
        return yield* failUpload("VerificationFailed");
    }),
  );
});
export const snapshot = Effect.fnUntraced(function* (
  storage: StorageService,
  record: UploadRecord,
) {
  if (!storage.capabilities.putConditions.includes("create-only"))
    return yield* failUpload("Unsupported");
  const existing = yield* storage
    .head({ key: record.publishedKey })
    .pipe(Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(undefined)));
  if (existing) {
    yield* verify(storage, existing, record);
    return existing;
  }
  const source = yield* storage.head({ key: record.stagingKey });
  yield* verify(storage, source, record);
  const object = yield* Effect.scoped(
    Effect.gen(function* () {
      const download = yield* storage.get(yield* pinned(source));
      if (download._tag !== "Object") return yield* failUpload("VerificationFailed");
      return yield* storage.put({
        key: record.publishedKey,
        body: download.body,
        contentLength: record.input.file.size,
        maxBytes: record.input.file.size,
        contentType: record.input.file.contentType,
        condition: { _tag: "CreateOnly" },
      });
    }),
  );
  yield* verify(storage, object, record);
  return object;
});
const removeOwned = Effect.fnUntraced(function* (storage: StorageService, object: ObjectMetadata) {
  if (object.ref.version) {
    yield* storage.delete({ ref: object.ref });
    return true;
  }
  if (object.etag && storage.capabilities.deleteConditions.includes("if-match")) {
    yield* storage.delete({ ref: object.ref, condition: { _tag: "IfMatch", etag: object.etag } });
    return true;
  }
  return false;
});
export const cleanup = Effect.fnUntraced(function* (storage: StorageService, record: UploadRecord) {
  const stage = yield* storage
    .head({ key: record.stagingKey })
    .pipe(Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(undefined)));
  const stagingCleaned = stage ? yield* removeOwned(storage, stage) : true;
  if (!["aborted", "rejected"].includes(record.state)) return stagingCleaned;
  const final =
    record.object ??
    (yield* storage
      .head({ key: record.publishedKey })
      .pipe(Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(undefined))));
  if (!final) return stagingCleaned;
  yield* verify(storage, final, record);
  return (yield* removeOwned(storage, final)) && stagingCleaned;
});
