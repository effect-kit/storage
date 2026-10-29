import { Cause, Effect, Stream } from "effect";
import type { Scope } from "effect";

import { StorageError, storageError } from "../errors.js";
import type { ObjectMetadata } from "../object.js";
import { resolveRange } from "../ranges.js";
import type { GetRequest, GetResult, HeadRequest, PutRequest } from "../requests.js";
import type { R2BucketTransport } from "./binding.js";
import { uploadBody } from "./body.js";
import { call, conditionalEtag, headers, metadata, validateKey } from "./mapping.js";

export const singleUploadBytes = 5 * 1024 ** 3 - 5 * 1024 ** 2;
export const objectBytes = 5 * 1024 ** 4 - 5 * 1024 ** 3;

export const put = Effect.fnUntraced(function* <E, R>(
  bucket: R2BucketTransport,
  storeId: string,
  unknownLimit: number,
  request: PutRequest<E, R>,
): Effect.fn.Return<ObjectMetadata, StorageError | E, R> {
  yield* validateKey(request.key, "put");
  const options = headers(request);
  if (new TextEncoder().encode(JSON.stringify(options)).length > 8192)
    return yield* storageError(
      "put",
      "r2-workers",
      { _tag: "InvalidInput", field: "metadata" },
      "not-started",
    );
  return yield* uploadBody(request, singleUploadBytes, unknownLimit, (body) =>
    call(
      "put",
      () =>
        bucket.put(request.key, body, {
          ...options,
          ...(request.condition
            ? {
                onlyIf:
                  request.condition._tag === "CreateOnly"
                    ? { etagDoesNotMatch: "*" }
                    : { etagMatches: conditionalEtag(request.condition.etag) },
              }
            : {}),
        }),
      "unknown",
    ).pipe(
      Effect.flatMap((object) =>
        object
          ? metadata(storeId, object).pipe(
              Effect.mapError(
                (error) =>
                  new StorageError({ ...error, operation: "put", mutationOutcome: "succeeded" }),
              ),
            )
          : Effect.fail(
              storageError("put", "r2-workers", { _tag: "PreconditionFailed" }, "rejected"),
            ),
      ),
    ),
  );
});

export const head = Effect.fnUntraced(function* (
  bucket: R2BucketTransport,
  storeId: string,
  request: HeadRequest,
) {
  const key = "ref" in request ? request.ref.key : request.key;
  yield* validateKey(key, "head");
  const object = yield* call("head", () => bucket.head(key));
  if (!object) return yield* storageError("head", "r2-workers", { _tag: "NotFound" });
  return yield* metadata(storeId, object);
});

export const get = Effect.fnUntraced(function* (
  bucket: R2BucketTransport,
  storeId: string,
  request: GetRequest,
): Effect.fn.Return<GetResult, StorageError, Scope.Scope> {
  const key = "ref" in request ? request.ref.key : request.key;
  yield* validateKey(key, "get");
  const range = request.range;
  const object = yield* Effect.acquireRelease(
    call("get", () =>
      bucket.get(key, {
        ...(request.condition
          ? {
              onlyIf:
                request.condition._tag === "IfMatch"
                  ? { etagMatches: conditionalEtag(request.condition.etag) }
                  : { etagDoesNotMatch: conditionalEtag(request.condition.etag) },
            }
          : {}),
        ...(range
          ? {
              range:
                range._tag === "Bounded"
                  ? { offset: range.start, length: range.end - range.start + 1 }
                  : range._tag === "From"
                    ? { offset: range.start }
                    : { suffix: range.length },
            }
          : {}),
      }),
    ),
    (response) =>
      response?.body && !response.body.locked
        ? call("get.close", () =>
            response.body ? response.body.cancel() : Promise.resolve(),
          ).pipe(Effect.orDie)
        : Effect.void,
  );
  if (!object) return yield* storageError("get", "r2-workers", { _tag: "NotFound" });
  const saved = yield* metadata(storeId, object);
  if (!object.body) {
    if (request.condition?._tag === "IfNoneMatch") return { _tag: "NotModified", metadata: saved };
    return yield* storageError(
      "get",
      "r2-workers",
      request.condition?._tag === "IfMatch"
        ? { _tag: "PreconditionFailed" }
        : { _tag: "ProviderFailure", code: "MissingBody" },
    );
  }
  const resolved = range
    ? yield* Effect.fromResult(resolveRange(range, object.size, "r2-workers"))
    : undefined;
  if (
    resolved &&
    (object.range?.offset !== resolved.start ||
      object.range.length !== resolved.end - resolved.start + 1)
  )
    return yield* storageError("get", "r2-workers", {
      _tag: "ProviderFailure",
      code: "InvalidRange",
    });
  const body = object.body;
  return {
    _tag: "Object",
    metadata: saved,
    contentLength: resolved ? resolved.end - resolved.start + 1 : object.size,
    range: resolved,
    body: Stream.fromPull(
      Effect.gen(function* () {
        const reader = yield* Effect.acquireRelease(
          Effect.sync(() => body.getReader()),
          (owned) =>
            call("get.body.close", () => owned.cancel()).pipe(
              Effect.ensuring(Effect.sync(() => owned.releaseLock())),
              Effect.orDie,
            ),
        );
        return Effect.gen(function* () {
          const chunk = yield* call("get.body", () => reader.read());
          if (chunk.done) return yield* Cause.done();
          if (!chunk.value)
            return yield* storageError("get.body", "r2-workers", { _tag: "TransportFailure" });
          return [chunk.value] as const;
        });
      }),
    ),
  };
});
