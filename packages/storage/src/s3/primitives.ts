import { Readable } from "node:stream";

import { Effect, Predicate, Schema, Stream } from "effect";
import type { Scope } from "effect";

import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import type { PutObjectCommandInput } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import type { ObjectMetadata } from "../object.js";
import { resolveRange } from "../ranges.js";
import { PutRequest } from "../requests.js";
import type { GetRequest, GetResult, HeadRequest, DeleteRequest } from "../requests.js";
import { Storage } from "../storage.js";
import { authority, encryptionHeaders, readEncryption } from "./config.js";
import type { S3Connection } from "./config.js";
import { listPage } from "./listing.js";
import { PutOptions, nativeLimits } from "./native-models.js";
import { condition, headers, metadata, refError, target } from "./object-mapping.js";
import { enforceProfile } from "./profile.js";
import { mapS3Error, request } from "./transport.js";
import { uploadBody } from "./upload-body.js";
import { S3Key, validateHeaders } from "./validation.js";

export type S3PutOptions = Pick<
  PutObjectCommandInput,
  | "ChecksumAlgorithm"
  | "ChecksumCRC32"
  | "ChecksumCRC32C"
  | "ChecksumCRC64NVME"
  | "ChecksumSHA1"
  | "ChecksumSHA256"
  | "ChecksumSHA512"
  | "ChecksumMD5"
  | "ChecksumXXHASH64"
  | "ChecksumXXHASH3"
  | "ChecksumXXHASH128"
  | "StorageClass"
  | "Tagging"
>;

const verifyTarget = Effect.fnUntraced(function* (
  connection: S3Connection,
  input: HeadRequest,
  operation: string,
) {
  yield* enforceProfile(connection, operation, input);
  if ("ref" in input) {
    const error = refError(input.ref, connection.config, operation);
    if (error) return yield* error;
    yield* decodeRequest(S3Key, input.ref.key, operation, "s3");
  } else {
    yield* decodeRequest(S3Key, input.key, operation, "s3");
  }
});

export const putObject = Effect.fnUntraced(function* <E = never, R = never>(
  connection: S3Connection,
  input: PutRequest<E, R>,
  options: S3PutOptions = {},
) {
  const value = yield* decodeRequest(
    Schema.Struct({ ...PutRequest.fields, key: S3Key }),
    input,
    "put",
    "s3",
  );
  yield* validateHeaders(
    {
      contentType: value.contentType,
      contentEncoding: value.contentEncoding,
      contentLanguage: value.contentLanguage,
      contentDisposition: value.contentDisposition,
      cacheControl: value.cacheControl,
      customMetadata: value.customMetadata,
    },
    "put",
  );
  const settings = yield* decodeRequest(PutOptions, options, "put", "s3");
  yield* enforceProfile(connection, "put", { ...value, checksum: settings });
  if (
    connection.profile &&
    ((settings.StorageClass &&
      !connection.profile.storageClasses.includes(settings.StorageClass)) ||
      (settings.Tagging && !connection.profile.tagging))
  )
    return yield* storageError(
      "put",
      connection.profile.provider,
      { _tag: "Unsupported", capability: "storage-class-or-tagging" },
      "not-started",
    );
  const maximum =
    connection.profile?.limits.maximumSinglePutBytes ?? nativeLimits.maximumSinglePutBytes;
  const output = yield* uploadBody(
    connection,
    {
      ...value,
      body: input.body,
      maxBytes: Math.min(value.maxBytes ?? maximum, maximum),
    },
    "put",
    (body, length) =>
      request(connection, "put", true, (signal) =>
        connection.client.send(
          new PutObjectCommand({
            ...settings,
            ...authority(connection.config),
            ...encryptionHeaders(connection.config.encryption),
            ...headers(value),
            ...condition(value.condition),
            Key: input.key,
            Body: body,
            ContentLength: length,
          }),
          { abortSignal: signal },
        ),
      ).pipe(Effect.map((result) => ({ result, length }))),
  );

  // A racing HEAD would identify a different write; PUT only reports fields observed by this request.
  return {
    ...metadata(connection.config, input.key, {
      ...output.result,
      ...headers(input),
      ContentLength: output.length,
    }),
  } satisfies ObjectMetadata;
});

const head = Effect.fnUntraced(function* (connection: S3Connection, input: HeadRequest) {
  yield* verifyTarget(connection, input, "head");
  const selected = target(input);
  const output = yield* request(connection, "head", false, (signal) =>
    connection.client.send(
      new HeadObjectCommand({
        ...authority(connection.config),
        ...readEncryption(connection.config.encryption),
        ...selected,
        ...(connection.readChecksums === false ? {} : { ChecksumMode: "ENABLED" as const }),
      }),
      { abortSignal: signal },
    ),
  );
  return metadata(connection.config, selected.Key, output);
});

const get = Effect.fnUntraced(function* (
  connection: S3Connection,
  input: GetRequest,
): Effect.fn.Return<GetResult, StorageError, Scope.Scope> {
  yield* verifyTarget(connection, input, "get");
  const selected = target(input);
  const range = input.range;
  const rangeHeader =
    range?._tag === "Bounded"
      ? `bytes=${String(range.start)}-${String(range.end)}`
      : range?._tag === "From"
        ? `bytes=${String(range.start)}-`
        : range?._tag === "Suffix"
          ? `bytes=-${String(range.length)}`
          : undefined;
  const response = yield* Effect.acquireRelease(
    request(connection, "get", false, (signal) =>
      connection.client.send(
        new GetObjectCommand({
          ...authority(connection.config),
          ...readEncryption(connection.config.encryption),
          ...selected,
          Range: rangeHeader,
          ...(connection.readChecksums === false ? {} : { ChecksumMode: "ENABLED" as const }),
          IfMatch: input.condition?._tag === "IfMatch" ? input.condition.etag : undefined,
          IfNoneMatch: input.condition?._tag === "IfNoneMatch" ? input.condition.etag : undefined,
        }),
        { abortSignal: signal },
      ),
    ).pipe(
      Effect.map((output) => ({ _tag: "Object" as const, output })),
      Effect.catchReason("StorageError", "NotModified", () =>
        Effect.succeed({ _tag: "NotModified" as const }),
      ),
    ),
    (acquired) => {
      if (acquired._tag !== "Object" || !acquired.output.Body) return Effect.void;
      const native = acquired.output.Body;
      if (native instanceof Readable)
        return Effect.sync(() => {
          native.destroy();
        });
      if (native instanceof ReadableStream && !native.locked)
        return Effect.tryPromise({ try: () => native.cancel(), catch: () => undefined }).pipe(
          Effect.catch(() => Effect.logWarning("S3 response cancellation failed")),
        );
      return Effect.void;
    },
    { interruptible: true },
  );

  if (response._tag === "NotModified") {
    return {
      _tag: "NotModified",
      metadata: {
        ref: "ref" in input ? input.ref : { storeId: connection.config.storeId, key: input.key },
      },
    };
  }
  const { output } = response;
  if (!output.Body)
    return yield* storageError("get", "s3", { _tag: "ProviderFailure", code: "MissingBody" });

  const native = output.Body;
  let body: Stream.Stream<Uint8Array, StorageError>;
  if (native instanceof Readable) {
    body = Stream.fromAsyncIterable(native, (cause) => mapS3Error(cause, "get", false));
  } else {
    const web = native.transformToWebStream();
    yield* Effect.addFinalizer(() =>
      web.locked
        ? Effect.void
        : Effect.tryPromise({
            try: () => web.cancel(),
            catch: () =>
              storageError("get", "s3", { _tag: "TransportFailure", code: "CleanupFailed" }),
          }).pipe(Effect.catch(() => Effect.logWarning("S3 response cancellation failed"))),
    );
    body = Stream.fromReadableStream({
      evaluate: () => web,
      onError: (cause) => mapS3Error(cause, "get", false),
    });
  }

  let resolved: { start: number; end: number; totalSize: number } | undefined;
  if (range) {
    const match = /^bytes (\d+)-(\d+)\/(\d+)$/u.exec(output.ContentRange ?? "");
    if (!match)
      return yield* storageError("get", "s3", {
        _tag: "ProviderFailure",
        code: "MissingContentRange",
      });
    const [start, end, totalSize] = match.slice(1).map(Number);
    if (
      start === undefined ||
      end === undefined ||
      totalSize === undefined ||
      ![start, end, totalSize].every(Number.isSafeInteger) ||
      start < 0 ||
      end < start ||
      end >= totalSize ||
      output.ContentLength !== end - start + 1
    )
      return yield* storageError("get", "s3", {
        _tag: "ProviderFailure",
        code: "InvalidContentRange",
      });
    resolved = { start, end, totalSize };
    const expected = yield* Effect.fromResult(resolveRange(range, totalSize, "s3"));
    if (expected.start !== start || expected.end !== end)
      return yield* storageError("get", "s3", {
        _tag: "ProviderFailure",
        code: "UnexpectedContentRange",
      });
  }

  let received = 0;
  body = body.pipe(
    Stream.mapEffect((chunk) =>
      Effect.gen(function* () {
        if (!Predicate.isUint8Array(chunk))
          return yield* storageError("get", "s3", {
            _tag: "ProviderFailure",
            code: "InvalidBodyChunk",
          });
        received += chunk.byteLength;
        if (output.ContentLength !== undefined && received > output.ContentLength)
          return yield* storageError("get", "s3", {
            _tag: "LengthMismatch",
            expected: output.ContentLength,
            actual: received,
          });
        return chunk;
      }),
    ),
    Stream.concat(
      Stream.drain(
        Stream.fromEffect(
          Effect.suspend(() =>
            output.ContentLength !== undefined && received !== output.ContentLength
              ? Effect.fail(
                  storageError("get", "s3", {
                    _tag: "LengthMismatch",
                    expected: output.ContentLength,
                    actual: received,
                  }),
                )
              : Effect.void,
          ),
        ),
      ),
    ),
  );

  return {
    _tag: "Object",
    metadata: metadata(connection.config, selected.Key, {
      ...output,
      ContentLength: resolved?.totalSize ?? output.ContentLength,
    }),
    contentLength: output.ContentLength,
    range: resolved,
    body,
  };
});

const remove = Effect.fnUntraced(function* (connection: S3Connection, input: DeleteRequest) {
  yield* verifyTarget(connection, input, "delete");
  yield* request(connection, "delete", true, (signal) =>
    connection.client.send(
      new DeleteObjectCommand({
        ...authority(connection.config),
        ...target(input),
        IfMatch: input.condition?.etag,
      }),
      { abortSignal: signal },
    ),
  );
});

export function primitives(connection: S3Connection) {
  return Storage.make({
    provider: connection.profile?.provider ?? "s3",
    storeId: connection.config.storeId,
    capabilities: connection.profile?.capabilities ?? {
      putConditions: ["create-only", "if-match"],
      getConditions: ["if-match", "if-none-match"],
      deleteConditions: ["if-match"],
      ranges: ["bounded", "from", "suffix"],
      versionedReads: "history",
      list: { maxPageSize: 1000, delimiter: true, cursorPersistence: "provider", snapshot: false },
    },
    put: (input) => putObject(connection, input),
    get: (input) => get(connection, input),
    head: (input) => head(connection, input),
    delete: (input) => remove(connection, input),
    listPage: (input) => listPage(connection, input),
  });
}
