import { Effect, Redacted, Schema } from "effect";

import * as Blob from "@vercel/blob";

import type { MultipartHandle, MultipartUploads } from "../../core/contracts/multipart.js";
import { storageError } from "../../core/errors.js";
import { ObjectRef } from "../../core/object.js";
import { webUploadBody } from "../../internal/web-upload-body.js";
import type { VercelConnection } from "./config.js";
import { metadata, validateKey, writeOptions } from "./mapping.js";
import { call } from "./transport.js";

const Handle = Schema.Struct({ key: Schema.String, uploadId: Schema.String });
export function multipart(connection: VercelConnection): MultipartUploads {
  const decode = Effect.fnUntraced(function* (upload: MultipartHandle) {
    yield* Schema.decodeUnknownEffect(
      Schema.Struct({ ref: ObjectRef, uploadId: Schema.Redacted(Schema.String) }),
    )(upload).pipe(
      Effect.mapError(() => storageError("multipart", "vercel", { _tag: "InvalidSession" })),
    );
    if (
      upload.ref.storeId !== `vercel:${connection.config.access}:${connection.config.storeId}` ||
      upload.ref.version
    )
      return yield* storageError("multipart", "vercel", { _tag: "InvalidSession" });
    yield* validateKey(upload.ref.key, "multipart");
    return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Handle))(
      Redacted.value(upload.uploadId),
    ).pipe(Effect.mapError(() => storageError("multipart", "vercel", { _tag: "InvalidSession" })));
  });
  return {
    limits: {
      minimumPartBytes: 5 * 1024 ** 2,
      maximumPartBytes: 256 * 1024 ** 2,
      maximumParts: 10000,
      completeConditions: [],
    },
    initiate: Effect.fnUntraced(function* (input) {
      yield* validateKey(input.key, "initiate");
      const headers = yield* writeOptions(input.headers ?? {}, "initiate");
      const result = yield* call(connection, "initiate", true, (options) =>
        Blob.createMultipartUpload(input.key, {
          ...options,
          ...headers,
          access: connection.config.access,
          allowOverwrite: true,
        }),
      );
      yield* Schema.decodeUnknownEffect(Handle)(result).pipe(
        Effect.mapError(() =>
          storageError("initiate", "vercel", { _tag: "ProviderFailure" }, "unknown"),
        ),
      );
      return {
        ref: {
          storeId: `vercel:${connection.config.access}:${connection.config.storeId}`,
          key: input.key,
        },
        uploadId: Redacted.make(JSON.stringify(result)),
      };
    }),
    uploadPart: Effect.fnUntraced(function* (input) {
      const handle = yield* decode(input.upload);
      if (
        !Number.isSafeInteger(input.partNumber) ||
        input.partNumber < 1 ||
        input.partNumber > 10000
      )
        return yield* storageError(
          "part",
          "vercel",
          { _tag: "InvalidInput", field: "partNumber" },
          "not-started",
        );
      return yield* webUploadBody(
        "vercel",
        {
          body: input.body,
          ...(input.contentLength !== undefined ? { contentLength: input.contentLength } : {}),
          maxBytes: 256 * 1024 ** 2,
        },
        connection.config.maxUnknownLengthBytes,
        (body, length) =>
          call(connection, "part", true, (options) =>
            Blob.uploadPart(
              input.upload.ref.key,
              body instanceof Uint8Array
                ? new ReadableStream({
                    start(controller) {
                      controller.enqueue(body);
                      controller.close();
                    },
                  })
                : body,
              {
                ...options,
                ...handle,
                access: connection.config.access,
                partNumber: input.partNumber,
              },
            ),
          ).pipe(Effect.map((part) => ({ ...part, size: length }))),
      );
    }),
    complete: Effect.fnUntraced(function* (input) {
      if (input.condition)
        return yield* storageError(
          "complete",
          "vercel",
          { _tag: "Unsupported", capability: "conditional-multipart-completion" },
          "not-started",
        );
      const handle = yield* decode(input.upload);
      yield* Schema.decodeUnknownEffect(
        Schema.Array(
          Schema.Struct({
            partNumber: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10000 })),
            etag: Schema.NonEmptyString,
            size: Schema.optional(Schema.Natural),
          }),
        ).check(Schema.isMaxLength(10000)),
      )(input.parts).pipe(
        Effect.mapError(() =>
          storageError(
            "complete",
            "vercel",
            { _tag: "InvalidInput", field: "parts" },
            "not-started",
          ),
        ),
      );
      if (
        input.parts.length === 0 ||
        input.parts.some((part, index) => part.partNumber !== index + 1)
      )
        return yield* storageError(
          "complete",
          "vercel",
          { _tag: "InvalidInput", field: "parts" },
          "not-started",
        );
      return yield* call(connection, "complete", true, (options) =>
        Blob.completeMultipartUpload(input.upload.ref.key, [...input.parts], {
          ...options,
          ...handle,
          access: connection.config.access,
          addRandomSuffix: false,
          allowOverwrite: true,
        }),
      ).pipe(
        Effect.flatMap((result) =>
          metadata(connection, result, "complete", input.upload.ref.key, true),
        ),
      );
    }),
    listParts: () =>
      Effect.fail(
        storageError("listParts", "vercel", {
          _tag: "Unsupported",
          capability: "multipart-inventory",
        }),
      ),
    abort: () =>
      Effect.fail(
        storageError("abort", "vercel", {
          _tag: "Unsupported",
          capability: "remote-multipart-abort",
        }),
      ),
  };
}
