import { Cause, DateTime, Effect, Predicate, Stream } from "effect";
import type { Scope } from "effect";
import type { PlatformError } from "effect/PlatformError";

import { storageError } from "../../core/errors.js";
import type { StorageError } from "../../core/errors.js";
import type { ObjectMetadata } from "../../core/object.js";
import { resolveRange } from "../../core/ranges.js";
import type { GetRequest, GetResult, HeadRequest, PutRequest } from "../../core/requests.js";
import { syncDirectory } from "../../runtime/filesystem.js";
import { encodeEnvelope, headerBytes, readChunk, readEnvelope } from "./envelope.js";
import { mapIO } from "./layout.js";
import type { FilesystemContext } from "./layout.js";

export const head = Effect.fnUntraced(function* (context: FilesystemContext, request: HeadRequest) {
  const key = "ref" in request ? request.ref.key : request.key;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const file = yield* context.open(yield* context.filename(key));
      return yield* readEnvelope(context, file, key);
    }),
  ).pipe(
    Effect.mapError((error) => (error._tag === "PlatformError" ? mapIO("head")(error) : error)),
  );
});

export const get = Effect.fnUntraced(function* (
  context: FilesystemContext,
  request: GetRequest,
): Effect.fn.Return<GetResult, StorageError, Scope.Scope> {
  const key = "ref" in request ? request.ref.key : request.key;
  const file = yield* context
    .open(yield* context.filename(key))
    .pipe(
      Effect.mapError((error) => (error._tag === "PlatformError" ? mapIO("get")(error) : error)),
    );
  const metadata = yield* readEnvelope(context, file, key).pipe(
    Effect.mapError((error) => (error._tag === "PlatformError" ? mapIO("get")(error) : error)),
  );
  if (
    request.condition?._tag === "IfMatch" &&
    request.condition.etag !== "*" &&
    request.condition.etag !== metadata.etag
  )
    return yield* storageError("get", "filesystem", { _tag: "PreconditionFailed" });
  if (
    request.condition?._tag === "IfNoneMatch" &&
    (request.condition.etag === "*" || request.condition.etag === metadata.etag)
  )
    return { _tag: "NotModified", metadata };

  const size = metadata.size ?? 0;
  const range = request.range
    ? yield* Effect.fromResult(resolveRange(request.range, size, "filesystem"))
    : undefined;
  const start = range?.start ?? 0;
  const length = range ? range.end - range.start + 1 : size;
  let offset = 0;
  const body = Stream.fromPull(
    Effect.succeed(
      Effect.gen(function* () {
        if (offset >= length) return yield* Cause.done();
        const bytes = yield* readChunk(
          file,
          headerBytes + start + offset,
          Math.min(65536, length - offset),
        ).pipe(
          Effect.mapError((error) =>
            error._tag === "PlatformError" ? mapIO("get")(error) : error,
          ),
        );
        offset += bytes.length;
        return [bytes] as const;
      }),
    ),
  );
  return { _tag: "Object", metadata, body, contentLength: length, range };
});

export const put = Effect.fnUntraced(function* <E, R>(
  context: FilesystemContext,
  request: PutRequest<E, R>,
): Effect.fn.Return<ObjectMetadata, StorageError | E, R> {
  const { fs, path } = context;
  const headers = {
    contentType: request.contentType,
    contentEncoding: request.contentEncoding,
    contentLanguage: request.contentLanguage,
    contentDisposition: request.contentDisposition,
    cacheControl: request.cacheControl,
    customMetadata: request.customMetadata ? { ...request.customMetadata } : undefined,
  };
  const limit = Math.min(context.maxObjectBytes, request.maxBytes ?? context.maxObjectBytes);
  if (new TextEncoder().encode(request.key).length > 4096)
    return yield* storageError(
      "put",
      "filesystem",
      { _tag: "InvalidInput", field: "key" },
      "not-started",
    );
  if (new TextEncoder().encode(JSON.stringify(headers)).length > 32768)
    return yield* storageError(
      "put",
      "filesystem",
      { _tag: "InvalidInput", field: "metadata" },
      "not-started",
    );
  if (request.contentLength !== undefined && request.contentLength > limit)
    return yield* storageError(
      "put",
      "filesystem",
      { _tag: "LimitExceeded", limit },
      "not-started",
    );

  yield* context.checkDirectories;
  const destination = yield* context.filename(request.key);
  const temporary = path.join(context.staging, `upload-${yield* context.random.uuidV4()}.pending`);
  let outcome: "not-started" | "unknown" | "succeeded" = "not-started";
  const map = (error: PlatformError) => mapIO("put", outcome)(error);

  return yield* Effect.acquireUseRelease(
    Effect.succeed(temporary),
    () =>
      Effect.gen(function* () {
        const metadata = yield* Effect.scoped(
          Effect.gen(function* () {
            const file = yield* fs
              .open(temporary, { flag: "wx+", mode: 0o600, noFollow: true })
              .pipe(Effect.mapError(map));
            yield* file.seek(BigInt(headerBytes), "start").pipe(Effect.mapError(map));
            const hash = context.hash.create();
            let length = 0;
            const source = Predicate.isUint8Array(request.body)
              ? Stream.succeed(request.body)
              : request.body;
            yield* Stream.runForEach(source, (chunk) =>
              Effect.gen(function* () {
                if (!Predicate.isUint8Array(chunk))
                  return yield* storageError(
                    "put",
                    "filesystem",
                    { _tag: "InvalidInput", field: "body" },
                    "not-started",
                  );
                if (chunk.length > limit - length)
                  return yield* storageError(
                    "put",
                    "filesystem",
                    { _tag: "LimitExceeded", limit },
                    "not-started",
                  );
                // Bound adapter-owned writes; the caller retains ownership of upstream allocations.
                for (let offset = 0; offset < chunk.length; offset += 65536) {
                  const bytes = chunk.subarray(offset, offset + 65536);
                  hash.update(bytes);
                  yield* file.writeAll(bytes).pipe(Effect.uninterruptible, Effect.mapError(map));
                }
                length += chunk.length;
              }),
            );
            if (request.contentLength !== undefined && request.contentLength !== length)
              return yield* storageError(
                "put",
                "filesystem",
                { _tag: "LengthMismatch", expected: request.contentLength, actual: length },
                "not-started",
              );

            const saved: ObjectMetadata = {
              ...headers,
              ref: { storeId: context.storeId, key: request.key },
              size: length,
              etag: `"${yield* context.random.uuidV4()}"`,
              lastModified: yield* DateTime.now,
              checksums: [hash.finish()],
            };
            const header = yield* encodeEnvelope(saved);
            yield* file.seek(0n, "start").pipe(Effect.mapError(map));
            yield* file.writeAll(header).pipe(Effect.uninterruptible, Effect.mapError(map));
            yield* file.sync.pipe(Effect.uninterruptible, Effect.mapError(map));
            return saved;
          }),
        );

        // Complete-file publication and durability barriers must join before temp cleanup.
        yield* Effect.gen(function* () {
          yield* context.checkDirectories;
          yield* context.regularFile(destination).pipe(
            Effect.asVoid,
            Effect.catchReason("PlatformError", "NotFound", () => Effect.void),
            Effect.mapError((error) => (error._tag === "PlatformError" ? map(error) : error)),
          );
          outcome = "unknown";
          if (request.condition?._tag === "CreateOnly") {
            yield* fs.link(temporary, destination).pipe(
              Effect.catchReason("PlatformError", "AlreadyExists", () =>
                storageError("put", "filesystem", { _tag: "PreconditionFailed" }, "rejected"),
              ),
              Effect.mapError((error) => (error._tag === "PlatformError" ? map(error) : error)),
            );
          } else yield* fs.rename(temporary, destination).pipe(Effect.mapError(map));
          yield* syncDirectory(fs, context.objects).pipe(Effect.mapError(map));
          outcome = "succeeded";
        }).pipe(Effect.uninterruptible);
        return metadata;
      }),
    () =>
      fs
        .remove(temporary, { force: true })
        .pipe(
          Effect.andThen(syncDirectory(fs, context.staging)),
          Effect.mapError(map),
          Effect.orDie,
        ),
  );
});
