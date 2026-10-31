import { Effect, Redacted, Schema } from "effect";

import type { MultipartUploads } from "../../core/contracts/multipart.js";
import { StorageError, storageError } from "../../core/errors.js";
import { ObjectHeaders, ObjectKey, ObjectRef, ObjectSize } from "../../core/object.js";
import type { R2BucketTransport } from "./binding.js";
import { uploadBody } from "./body.js";
import { bindingError, call, headers, metadata, validateKey } from "./mapping.js";
import { singleUploadBytes } from "./primitives.js";

const Handle = Schema.Struct({ ref: ObjectRef, uploadId: Schema.Redacted(Schema.NonEmptyString) });
const Part = Schema.Struct({
  partNumber: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10000 })),
  etag: Schema.NonEmptyString,
});

export function multipart(
  bucket: R2BucketTransport,
  storeId: string,
  unknownLimit: number,
): MultipartUploads {
  const validate = Effect.fnUntraced(function* (input: unknown) {
    const upload = yield* Schema.decodeUnknownEffect(Handle)(input).pipe(
      Effect.mapError(() =>
        storageError(
          "multipart.handle",
          "r2-workers",
          { _tag: "InvalidInput", field: "upload" },
          "not-started",
        ),
      ),
    );
    if (upload.ref.storeId !== storeId || upload.ref.version)
      return yield* storageError(
        "multipart.handle",
        "r2-workers",
        { _tag: "InvalidInput", field: "upload.ref" },
        "not-started",
      );
    yield* validateKey(upload.ref.key, "multipart.handle");
    return upload;
  });
  return {
    limits: {
      minimumPartBytes: 5 * 1024 ** 2,
      maximumPartBytes: singleUploadBytes,
      maximumParts: 10000,
      completeConditions: [],
    },
    initiate: Effect.fnUntraced(function* (request) {
      yield* Schema.decodeUnknownEffect(ObjectKey)(request.key).pipe(
        Effect.mapError(() =>
          storageError(
            "multipart.initiate",
            "r2-workers",
            { _tag: "InvalidInput", field: "key" },
            "not-started",
          ),
        ),
      );
      yield* validateKey(request.key, "multipart.initiate");
      const selected = yield* Schema.decodeUnknownEffect(ObjectHeaders)(request.headers ?? {}).pipe(
        Effect.mapError(() =>
          storageError(
            "multipart.initiate",
            "r2-workers",
            { _tag: "InvalidInput", field: "headers" },
            "not-started",
          ),
        ),
      );
      const upload = yield* call(
        "multipart.initiate",
        () => bucket.createMultipartUpload(request.key, headers(selected)),
        "unknown",
      );
      return { ref: { storeId, key: request.key }, uploadId: Redacted.make(upload.uploadId) };
    }),
    uploadPart: Effect.fnUntraced(function* (request) {
      const upload = yield* validate(request.upload);
      yield* Schema.decodeUnknownEffect(Part.fields.partNumber)(request.partNumber).pipe(
        Effect.mapError(() =>
          storageError(
            "multipart.part",
            "r2-workers",
            { _tag: "InvalidInput", field: "partNumber" },
            "not-started",
          ),
        ),
      );
      const native = yield* Effect.try({
        try: () => bucket.resumeMultipartUpload(upload.ref.key, Redacted.value(upload.uploadId)),
        catch: bindingError("multipart.resume", "not-started"),
      });
      if (request.contentLength !== undefined)
        yield* Schema.decodeUnknownEffect(ObjectSize)(request.contentLength).pipe(
          Effect.mapError(() =>
            storageError(
              "multipart.part",
              "r2-workers",
              { _tag: "InvalidInput", field: "contentLength" },
              "not-started",
            ),
          ),
        );
      const part = yield* uploadBody(request, singleUploadBytes, unknownLimit, (body) =>
        call("multipart.part", () => native.uploadPart(request.partNumber, body), "unknown"),
      );
      return {
        partNumber: part.partNumber,
        etag: part.etag,
        ...(request.contentLength === undefined ? {} : { size: request.contentLength }),
      };
    }),
    listParts: () =>
      Effect.fail(
        storageError("multipart.listParts", "r2-workers", {
          _tag: "Unsupported",
          capability: "multipart-part-inventory",
        }),
      ),
    complete: Effect.fnUntraced(function* (request) {
      const upload = yield* validate(request.upload);
      if (request.condition)
        return yield* storageError(
          "multipart.complete",
          "r2-workers",
          { _tag: "Unsupported", capability: "multipart-conditional-completion" },
          "not-started",
        );
      const parts = yield* Schema.decodeUnknownEffect(
        Schema.Array(Part).check(Schema.isMinLength(1), Schema.isMaxLength(10000)),
      )(request.parts).pipe(
        Effect.mapError(() =>
          storageError(
            "multipart.complete",
            "r2-workers",
            { _tag: "InvalidInput", field: "parts" },
            "not-started",
          ),
        ),
      );
      if (new Set(parts.map((part) => part.partNumber)).size !== parts.length)
        return yield* storageError(
          "multipart.complete",
          "r2-workers",
          { _tag: "InvalidInput", field: "parts" },
          "not-started",
        );
      const native = yield* Effect.try({
        try: () => bucket.resumeMultipartUpload(upload.ref.key, Redacted.value(upload.uploadId)),
        catch: bindingError("multipart.resume", "not-started"),
      });
      const object = yield* call(
        "multipart.complete",
        () => native.complete(parts.toSorted((a, b) => a.partNumber - b.partNumber)),
        "unknown",
      );
      return yield* metadata(storeId, object).pipe(
        Effect.mapError(
          (error) =>
            new StorageError({
              ...error,
              operation: "multipart.complete",
              mutationOutcome: "succeeded",
            }),
        ),
      );
    }),
    abort: Effect.fnUntraced(function* (request) {
      const upload = yield* validate(request);
      const native = yield* Effect.try({
        try: () => bucket.resumeMultipartUpload(upload.ref.key, Redacted.value(upload.uploadId)),
        catch: bindingError("multipart.resume", "not-started"),
      });
      yield* call("multipart.abort", () => native.abort(), "unknown").pipe(
        Effect.catchReason("StorageError", "NotFound", () => Effect.void),
      );
    }),
  };
}
