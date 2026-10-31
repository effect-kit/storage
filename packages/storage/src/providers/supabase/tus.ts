import { Effect, Redacted, Schema } from "effect";
import { Base64 } from "effect/encoding";

import type {
  OffsetUploads,
  ResumableHandle,
  ResumableState,
} from "../../core/contracts/resumable.js";
import { storageError } from "../../core/errors.js";
import { ObjectRef } from "../../core/object.js";
import { webUploadBody } from "../../internal/web-upload-body.js";
import type { SupabaseConnection } from "./config.js";
import { nativeHeaders, validateKey } from "./mapping.js";
import { head } from "./primitives.js";
import { request } from "./transport.js";

const chunkBytes = 6 * 1024 ** 2;
const Handle = Schema.Struct({
  url: Schema.String,
  total: Schema.Natural,
  operationId: Schema.optional(Schema.String),
});
const fail = () => storageError("tus", "supabase", { _tag: "InvalidSession" });
const sessionUrl = Effect.fnUntraced(function* (connection: SupabaseConnection, value: string) {
  const url = URL.parse(value, connection.config.baseUrl);
  if (
    !url ||
    url.origin !== connection.config.url ||
    !url.pathname.startsWith("/storage/v1/upload/resumable/") ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    return yield* fail();
  return url.href;
});

/** One HTTP exchange per operation. The shared engine owns retries, source bytes and checkpoints. */
export function tusUploads(
  connection: SupabaseConnection,
  extensions: ReadonlyArray<string>,
): OffsetUploads {
  const decode = Effect.fnUntraced(function* (upload: ResumableHandle) {
    yield* Schema.decodeUnknownEffect(
      Schema.Struct({
        protocol: Schema.Literal("supabase-tus-v1"),
        ref: ObjectRef,
        handle: Schema.Redacted(Schema.String),
      }),
    )(upload).pipe(Effect.mapError(fail));
    if (upload.ref.storeId !== connection.config.storeId || upload.ref.version)
      return yield* fail();
    yield* validateKey(upload.ref.key, "tus");
    const handle = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Handle))(
      Redacted.value(upload.handle),
    ).pipe(Effect.mapError(fail));
    return { ...handle, url: yield* sessionUrl(connection, handle.url) };
  });
  const state = Effect.fnUntraced(function* (
    upload: ResumableHandle,
    headers: Readonly<Record<string, string>>,
    status: number,
  ): Effect.fn.Return<ResumableState, ReturnType<typeof storageError>> {
    if (status === 404 || status === 410) return { _tag: "Expired" };
    const handle = yield* decode(upload);
    const offset = Number(headers["upload-offset"]);
    if (
      headers["tus-resumable"] !== "1.0.0" ||
      headers["upload-offset"] === undefined ||
      !Number.isSafeInteger(offset) ||
      offset < 0 ||
      offset > handle.total ||
      (headers["upload-length"] !== undefined && Number(headers["upload-length"]) !== handle.total)
    )
      return yield* fail();
    if (offset !== handle.total) return { _tag: "Uploading", committedOffset: offset };
    return { _tag: "Completed", metadata: { ref: upload.ref, size: handle.total } };
  });
  return {
    limits: {
      alignmentBytes: chunkBytes,
      maximumChunkBytes: chunkBytes,
      fixedChunkBytes: chunkBytes,
    },
    create: Effect.fnUntraced(function* (input) {
      yield* validateKey(input.key, "initiate");
      yield* nativeHeaders(input, "initiate");
      if (
        input.contentLength === undefined ||
        !Number.isSafeInteger(input.contentLength) ||
        input.contentLength < 0 ||
        input.contentEncoding !== undefined ||
        (input.cacheControl !== undefined && !/^max-age=\d+$/u.test(input.cacheControl)) ||
        input.contentLength > connection.config.maxObjectBytes ||
        input.condition?._tag === "IfMatch"
      )
        return yield* storageError(
          "initiate",
          "supabase",
          { _tag: "InvalidInput", field: "length-or-condition" },
          "not-started",
        );
      const values = {
        bucketName: connection.config.bucket,
        objectName: input.key,
        contentType: input.contentType ?? "application/octet-stream",
        cacheControl: input.cacheControl?.replace(/^max-age=/u, "") ?? "3600",
        metadata: JSON.stringify(input.customMetadata ?? {}),
      };
      const metadata = Object.entries(values)
        .map(([key, value]) => `${key} ${Base64.encode(new TextEncoder().encode(value))}`)
        .join(",");
      const response = yield* request(connection, "initiate", "POST", "/upload/resumable", {
        headers: {
          "tus-resumable": "1.0.0",
          "upload-length": String(input.contentLength),
          "upload-metadata": metadata,
          "x-upsert": String(input.condition?._tag !== "CreateOnly"),
        },
      }).pipe(Effect.scoped);
      const url = yield* sessionUrl(connection, response.headers.location ?? "");
      return {
        protocol: "supabase-tus-v1",
        ref: { storeId: connection.config.storeId, key: input.key },
        handle: Redacted.make(
          JSON.stringify({
            url,
            total: input.contentLength,
            operationId: input.customMetadata?.["effect-storage-operation"],
          }),
        ),
      };
    }),
    query: Effect.fnUntraced(function* (upload) {
      const handle = yield* decode(upload);
      const response = yield* request(connection, "query", "HEAD", handle.url, {
        headers: { "tus-resumable": "1.0.0" },
        accepted: [404, 410],
      }).pipe(Effect.scoped);
      // Completion needs object metadata as a separate protocol operation; see completion state below.
      return yield* state(upload, response.headers, response.status);
    }),
    write: Effect.fnUntraced(function* (input) {
      const handle = yield* decode(input.upload);
      if (
        !Number.isSafeInteger(input.offset) ||
        input.offset < 0 ||
        input.offset + input.contentLength > handle.total ||
        !Number.isSafeInteger(input.contentLength) ||
        input.contentLength < 0 ||
        input.contentLength > chunkBytes ||
        input.final !== (input.offset + input.contentLength === handle.total)
      )
        return yield* fail();
      return yield* webUploadBody(
        "supabase",
        { body: input.body, contentLength: input.contentLength, maxBytes: chunkBytes },
        chunkBytes,
        (body) =>
          Effect.gen(function* () {
            const response = yield* request(connection, "write", "PATCH", handle.url, {
              body,
              headers: {
                "tus-resumable": "1.0.0",
                "upload-offset": String(input.offset),
                "content-type": "application/offset+octet-stream",
              },
              accepted: [404, 410],
            }).pipe(Effect.scoped);
            return yield* state(input.upload, response.headers, response.status);
          }),
      );
    }),
    resolveCompletion: Effect.fnUntraced(function* (upload) {
      const handle = yield* decode(upload);
      const metadata = yield* head(connection, { ref: upload.ref });
      if (
        metadata.size !== handle.total ||
        !metadata.etag ||
        (handle.operationId &&
          metadata.customMetadata?.["effect-storage-operation"] !== handle.operationId)
      )
        return yield* storageError(
          "tus.complete",
          "supabase",
          { _tag: "IntegrityMismatch" },
          "unknown",
        );
      return metadata;
    }),
    cancel: Effect.fnUntraced(function* (upload) {
      const handle = yield* decode(upload);
      if (!extensions.includes("termination")) {
        const response = yield* request(connection, "cancel.query", "HEAD", handle.url, {
          headers: { "tus-resumable": "1.0.0" },
          accepted: [404, 410],
        }).pipe(Effect.scoped);
        if (response.status === 404 || response.status === 410) return;
        return yield* storageError(
          "cancel",
          "supabase",
          { _tag: "Unsupported", capability: "tus-termination" },
          "not-started",
        );
      }
      yield* request(connection, "cancel", "DELETE", handle.url, {
        headers: { "tus-resumable": "1.0.0" },
        accepted: [404, 410],
      }).pipe(Effect.scoped);
    }),
  };
}

export const negotiateTus = Effect.fnUntraced(function* (connection: SupabaseConnection) {
  const response = yield* request(connection, "tus.options", "OPTIONS", "/upload/resumable").pipe(
    Effect.scoped,
  );
  // Hosted gateways can answer OPTIONS themselves and omit the tus headers.
  // Explicit resumable mode may still use Supabase's documented creation protocol;
  // optional extensions such as termination must never be inferred from that response.
  if (
    response.headers["tus-version"] === undefined &&
    response.headers["tus-extension"] === undefined
  )
    return ["creation"];
  const extensions = (response.headers["tus-extension"] ?? "")
    .split(",")
    .map((value) => value.trim());
  if (
    !(response.headers["tus-version"] ?? "")
      .split(",")
      .map((value) => value.trim())
      .includes("1.0.0") ||
    !extensions.includes("creation")
  )
    return yield* storageError("tus.options", "supabase", {
      _tag: "Unsupported",
      capability: "tus-creation-v1",
    });
  return extensions;
});
