import { Effect, Redacted, Schema } from "effect";

import type {
  OffsetUploads,
  ResumableHandle,
  ResumableState,
} from "../../core/contracts/resumable.js";
import type { StorageError } from "../../core/errors.js";
import { storageError } from "../../core/errors.js";
import { ObjectRef } from "../../core/object.js";
import { uploadBody } from "../../internal/node/upload-body.js";
import type { GcsConnection } from "./config.js";
import { metadata, nativeHeaders } from "./mapping.js";
import { sessionUrl } from "./primitives.js";
import { jsonRequest } from "./transport.js";
import { validateKey } from "./validation.js";

const Handle = Schema.Struct({ url: Schema.String, total: Schema.Natural });
export function offsetUploads(connection: GcsConnection): OffsetUploads {
  const decode = Effect.fnUntraced(function* (upload: ResumableHandle) {
    yield* Schema.decodeUnknownEffect(
      Schema.Struct({
        protocol: Schema.String,
        ref: ObjectRef,
        handle: Schema.Redacted(Schema.String),
      }),
    )(upload).pipe(
      Effect.mapError(() => storageError("session", "gcs", { _tag: "InvalidSession" })),
    );
    if (
      upload.protocol !== "gcs-resumable-v1" ||
      upload.ref.storeId !== connection.config.storeId ||
      upload.ref.version
    )
      return yield* storageError("session", "gcs", { _tag: "InvalidSession" });
    const handle = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Handle))(
      Redacted.value(upload.handle),
    ).pipe(Effect.mapError(() => storageError("session", "gcs", { _tag: "InvalidSession" })));
    return { ...handle, url: yield* sessionUrl(connection, handle.url) };
  });
  const state = Effect.fnUntraced(function* (response: {
    status: number;
    headers: Record<string, string>;
    body: unknown;
  }): Effect.fn.Return<ResumableState, StorageError> {
    if (response.status === 404 || response.status === 410) return { _tag: "Expired" };
    if (response.status === 200 || response.status === 201)
      return {
        _tag: "Completed",
        metadata: yield* metadata(connection, response.body, "resumable"),
      };
    if (response.status !== 308) return { _tag: "Unknown" };
    const range = response.headers.range;
    if (!range) return { _tag: "Uploading", committedOffset: 0 };
    const match = /^bytes=0-(\d+)$/u.exec(range);
    const offset = match ? Number(match[1]) + 1 : NaN;
    if (!Number.isSafeInteger(offset))
      return yield* storageError("query", "gcs", { _tag: "ProviderFailure" });
    return { _tag: "Uploading", committedOffset: offset };
  });
  return {
    limits: { alignmentBytes: 256 * 1024, maximumChunkBytes: 256 * 1024 * 1024 },
    create: Effect.fnUntraced(function* (request) {
      yield* validateKey(request.key, "initiate", true);
      if (
        request.contentLength === undefined ||
        !Number.isSafeInteger(request.contentLength) ||
        request.contentLength < 0 ||
        request.condition?._tag === "IfMatch"
      )
        return yield* storageError(
          "initiate",
          "gcs",
          { _tag: "InvalidInput", field: "contentLength-or-condition" },
          "not-started",
        );
      const response = yield* jsonRequest(connection, "initiate", true, {
        uri: `${connection.config.apiEndpoint}/upload/storage/v1/b/${encodeURIComponent(connection.config.bucket)}/o`,
        method: "POST",
        json: { name: request.key, ...nativeHeaders(request) },
        qs: {
          uploadType: "resumable",
          name: request.key,
          ...(request.condition ? { ifGenerationMatch: "0" } : {}),
          ...(connection.config.kmsKeyName ? { kmsKeyName: connection.config.kmsKeyName } : {}),
        },
        headers: { "X-Upload-Content-Length": String(request.contentLength) },
      });
      const url = yield* sessionUrl(connection, response.headers.location);
      return {
        protocol: "gcs-resumable-v1",
        ref: { storeId: connection.config.storeId, key: request.key },
        handle: Redacted.make(JSON.stringify({ url, total: request.contentLength })),
      };
    }),
    query: Effect.fnUntraced(function* (upload) {
      const handle = yield* decode(upload);
      return yield* state(
        yield* jsonRequest(
          connection,
          "query",
          false,
          {
            uri: handle.url,
            method: "PUT",
            headers: { "Content-Length": "0", "Content-Range": `bytes */${handle.total}` },
            json: true,
          },
          [308, 404, 410],
        ),
      );
    }),
    write: Effect.fnUntraced(function* (request) {
      const handle = yield* decode(request.upload);
      const end = request.offset + request.contentLength;
      if (
        !Number.isSafeInteger(request.offset) ||
        request.offset < 0 ||
        !Number.isSafeInteger(request.contentLength) ||
        request.contentLength < 0 ||
        end > handle.total ||
        request.contentLength > 256 * 1024 * 1024 ||
        (request.final
          ? end !== handle.total
          : request.contentLength === 0 || request.contentLength % (256 * 1024) !== 0)
      )
        return yield* storageError(
          "write",
          "gcs",
          { _tag: "InvalidInput", field: "offset-length-final" },
          "not-started",
        );
      return yield* uploadBody(
        { provider: "gcs", maxUnknownLengthBytes: connection.config.maxUnknownLengthBytes },
        request,
        "write",
        (body, length) =>
          jsonRequest(
            connection,
            "write",
            true,
            {
              uri: handle.url,
              method: "PUT",
              body,
              json: true,
              headers: {
                "Content-Length": String(length),
                "Content-Range": length
                  ? `bytes ${request.offset}-${end - 1}/${request.final ? handle.total : "*"}`
                  : `bytes */${handle.total}`,
              },
            },
            [308, 404, 410],
          ).pipe(Effect.flatMap(state)),
      );
    }),
    cancel: Effect.fnUntraced(function* (upload) {
      const handle = yield* decode(upload);
      yield* jsonRequest(
        connection,
        "cancel",
        true,
        { uri: handle.url, method: "DELETE", headers: { "Content-Length": "0" } },
        [499, 404, 410],
      );
    }),
  };
}
