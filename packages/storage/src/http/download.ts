import { Effect, Scope, Stream } from "effect";
import { HttpServerResponse } from "effect/http";
import type { HttpServerRequest } from "effect/http";

import type { ObjectRef } from "../core/object.js";
import type { ByteRange } from "../core/ranges.js";
import type { StorageService } from "../core/storage.js";
import { failUpload } from "../uploads/schema.js";

export const privateDownload = Effect.fnUntraced(function* (
  storage: StorageService,
  request: HttpServerRequest.HttpServerRequest,
  target: { readonly ref: ObjectRef; readonly filename: string },
) {
  if (!target.filename || /[\r\n\0/\\]/u.test(target.filename))
    return yield* failUpload("BadRequest");
  const head = yield* storage.head({ ref: target.ref });
  const headers: Record<string, string> = {
    "cache-control": "private, no-store",
    "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(target.filename).replace(/['()*]/gu, (character) => `%${character.charCodeAt(0).toString(16)}`)}`,
    "x-content-type-options": "nosniff",
    "accept-ranges": "bytes",
    ...(head.etag ? { etag: head.etag } : {}),
  };
  const matches = (value: string | undefined) =>
    value
      ?.split(",")
      .some(
        (tag) =>
          tag.trim() === "*" ||
          tag.trim() === head.etag ||
          tag.trim().replace(/^W\//u, "") === head.etag,
      );
  if (
    request.headers["if-match"] &&
    !request.headers["if-match"]
      .split(",")
      .some(
        (tag) => tag.trim() === "*" || (!tag.trim().startsWith("W/") && tag.trim() === head.etag),
      )
  )
    return HttpServerResponse.empty({ status: 412, headers });
  if (matches(request.headers["if-none-match"]))
    return HttpServerResponse.empty({ status: 304, headers });
  let range: ByteRange | undefined;
  const raw = request.headers.range;
  if (raw && (!request.headers["if-range"] || request.headers["if-range"] === head.etag)) {
    const parsed = /^bytes=(\d*)-(\d*)$/u.exec(raw);
    if (
      !parsed ||
      (!parsed[1] && !parsed[2]) ||
      [parsed[1], parsed[2]].some((value) => value && !Number.isSafeInteger(Number(value)))
    )
      return HttpServerResponse.empty({
        status: 416,
        headers: { ...headers, "content-range": `bytes */${head.size}` },
      });
    range = parsed[1]
      ? parsed[2]
        ? { _tag: "Bounded", start: Number(parsed[1]), end: Number(parsed[2]) }
        : { _tag: "From", start: Number(parsed[1]) }
      : { _tag: "Suffix", length: Number(parsed[2]) };
    if (range._tag === "Suffix" && !range.length)
      return HttpServerResponse.empty({
        status: 416,
        headers: { ...headers, "content-range": `bytes */${head.size}` },
      });
  }
  const selected = {
    ref: head.ref,
    ...(head.ref.version
      ? {}
      : head.etag
        ? { condition: { _tag: "IfMatch" as const, etag: head.etag } }
        : {}),
  };
  if (!head.ref.version && !head.etag) return yield* failUpload("Unsupported");
  if (request.method === "HEAD")
    return HttpServerResponse.empty({
      status: 200,
      headers: {
        ...headers,
        "content-length": String(head.size),
        "content-type": "application/octet-stream",
      },
    });
  const result = yield* storage
    .get({ ...selected, ...(range ? { range } : {}) })
    .pipe(
      Effect.catchReason("StorageError", "RangeNotSatisfiable", () => Effect.succeed(undefined)),
    );
  if (!result)
    return HttpServerResponse.empty({
      status: 416,
      headers: { ...headers, "content-range": `bytes */${head.size}` },
    });
  if (result._tag !== "Object") return HttpServerResponse.empty({ status: 304, headers });
  const scope = yield* Scope.Scope;
  return HttpServerResponse.stream(result.body.pipe(Stream.provideService(Scope.Scope, scope)), {
    status: result.range ? 206 : 200,
    contentType: "application/octet-stream",
    ...(result.contentLength !== undefined ? { contentLength: result.contentLength } : {}),
    headers: {
      ...headers,
      ...(result.range
        ? {
            "content-range": `bytes ${result.range.start}-${result.range.end}/${result.range.totalSize}`,
          }
        : {}),
    },
  });
});
