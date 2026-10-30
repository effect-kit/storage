import { Context, Effect, FileSystem, Layer, Schema, Stream } from "effect";
import type { Scope } from "effect";

import { storageError } from "../errors.js";
import type { StorageError } from "../errors.js";
import { ObjectHeaders, type ObjectMetadata } from "../object.js";
import { StorageRepresentation } from "../representation.js";
import type { RepresentationCodec, RepresentationOptions } from "../representation.js";
import type { GetResult, HeadRequest, ListEntry, PutRequest } from "../requests.js";
import { sha256 } from "../runtime/portable-hash.js";
import { Storage } from "../storage.js";
import { TransferDriver } from "../transfers/dependencies.js";
import { digestText } from "../uploads/verification.js";
import { makeStaging } from "./staging.js";

const maximumHeaderBytes = 16384;
const Header = Schema.Struct({
  version: Schema.Literal(1),
  storeId: Schema.String,
  key: Schema.String,
  size: Schema.Natural,
  sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/u)),
  headers: ObjectHeaders,
});
const marker = "effect-storage-representation";
const fail = () => storageError("representation", "storage", { _tag: "IntegrityMismatch" });

export const makeRepresentation = Effect.fnUntraced(function* (
  codec: RepresentationCodec,
  options: RepresentationOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const source = yield* Storage;
  const stage = yield* makeStaging;
  if (
    !codec.format ||
    !Number.isSafeInteger(options.maxDecodedBytes) ||
    options.maxDecodedBytes < 1 ||
    !Number.isSafeInteger(options.maxStoredBytes) ||
    options.maxStoredBytes < 1 ||
    (options.maxExpansionRatio !== undefined &&
      (!Number.isFinite(options.maxExpansionRatio) || options.maxExpansionRatio < 1))
  )
    return yield* storageError("representation", source.provider, {
      _tag: "InvalidConfiguration",
      field: "limits",
    });
  const storeId = `representation:${digestText(JSON.stringify([source.storeId, codec.format]))}`;
  const targetIn = (target: HeadRequest): Effect.Effect<HeadRequest, StorageError> =>
    "ref" in target
      ? target.ref.storeId === storeId
        ? Effect.succeed({ ref: { ...target.ref, storeId: source.storeId } })
        : Effect.fail(fail())
      : Effect.succeed({ key: target.key });
  const logical = (stored: ObjectMetadata, header: typeof Header.Type): ObjectMetadata => ({
    ...header.headers,
    ref: { ...stored.ref, storeId },
    etag: stored.etag,
    lastModified: stored.lastModified,
    size: header.size,
    checksums: [
      { algorithm: "sha256", encoding: "hex", scope: "full-object", value: header.sha256 },
    ],
  });
  const decode = Effect.fnUntraced(function* (target: HeadRequest) {
    const stored = yield* source.head(yield* targetIn(target));
    if (
      stored.customMetadata?.[marker] !== codec.format ||
      stored.size === undefined ||
      stored.size > options.maxStoredBytes
    )
      return yield* fail();
    if (!stored.ref.version && !stored.etag)
      return yield* storageError("representation", source.provider, {
        _tag: "Unsupported",
        capability: "pinned-read",
      });
    const result = yield* source.get({
      ref: stored.ref,
      ...(!stored.ref.version && stored.etag
        ? { condition: { _tag: "IfMatch" as const, etag: stored.etag } }
        : {}),
    });
    if (result._tag !== "Object") return yield* fail();
    const decoded = yield* stage(
      codec.decode(result.body),
      Math.min(
        options.maxDecodedBytes + maximumHeaderBytes,
        options.maxExpansionRatio
          ? stored.size * options.maxExpansionRatio
          : Number.MAX_SAFE_INTEGER,
      ),
    );
    const prefix = new Uint8Array(Math.min(decoded.size, maximumHeaderBytes));
    let prefixOffset = 0;
    yield* fs.stream(decoded.path, { bytesToRead: maximumHeaderBytes }).pipe(
      Stream.runForEach((chunk) =>
        Effect.sync(() => {
          prefix.set(chunk, prefixOffset);
          prefixOffset += chunk.length;
        }),
      ),
      Effect.mapError(fail),
    );
    const newline = prefix.indexOf(10);
    if (newline < 0) return yield* fail();
    const header = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Header))(
      new TextDecoder().decode(prefix.subarray(0, newline)),
    ).pipe(Effect.mapError(fail));
    if (
      header.storeId !== storeId ||
      header.key !== stored.ref.key ||
      header.size > options.maxDecodedBytes ||
      decoded.size - newline - 1 !== header.size
    )
      return yield* fail();
    const actual = sha256.create();
    yield* Stream.runForEach(decoded.stream(newline + 1), (chunk) =>
      Effect.sync(() => actual.update(chunk)),
    );
    if (actual.finish().value !== header.sha256) return yield* fail();
    return { stored, metadata: logical(stored, header), body: decoded.stream(newline + 1) };
  });
  const put = Effect.fnUntraced(function* <E, R>(request: PutRequest<E, R>) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const raw = yield* stage(
          request.body instanceof Uint8Array ? Stream.succeed(request.body) : request.body,
          Math.min(options.maxDecodedBytes, request.maxBytes ?? options.maxDecodedBytes),
        );
        if (request.contentLength !== undefined && request.contentLength !== raw.size)
          return yield* storageError(
            "put",
            source.provider,
            { _tag: "LengthMismatch", expected: request.contentLength, actual: raw.size },
            "not-started",
          );
        const header = {
          version: 1 as const,
          storeId,
          key: request.key,
          size: raw.size,
          sha256: raw.checksum.value,
          headers: yield* Schema.decodeUnknownEffect(ObjectHeaders)(request).pipe(
            Effect.mapError(fail),
          ),
        };
        const prefix = new TextEncoder().encode(`${JSON.stringify(header)}\n`);
        if (prefix.length >= maximumHeaderBytes)
          return yield* storageError(
            "put",
            source.provider,
            { _tag: "LimitExceeded", limit: maximumHeaderBytes },
            "not-started",
          );
        const encoded = yield* stage(
          codec.encode(Stream.concat(Stream.succeed(prefix), raw.stream())),
          options.maxStoredBytes,
        );
        const stored = yield* source.put({
          key: request.key,
          body: encoded.stream(),
          contentLength: encoded.size,
          maxBytes: options.maxStoredBytes,
          contentType: "application/octet-stream",
          customMetadata: { [marker]: codec.format },
          ...(request.condition ? { condition: request.condition } : {}),
        });
        return logical(stored, header);
      }),
    );
  });
  const storage = Storage.make({
    provider: source.provider,
    storeId,
    capabilities: { ...source.capabilities, ranges: [] },
    put,
    head: (target) => Effect.scoped(decode(target).pipe(Effect.map((value) => value.metadata))),
    get: Effect.fnUntraced(function* (request): Effect.fn.Return<
      GetResult,
      StorageError,
      Scope.Scope
    > {
      if (request.range)
        return yield* storageError("get", source.provider, {
          _tag: "Unsupported",
          capability: "representation-range",
        });
      const value = yield* decode(request);
      if (
        request.condition?._tag === "IfMatch" &&
        request.condition.etag !== value.metadata.etag &&
        request.condition.etag !== "*"
      )
        return yield* storageError("get", source.provider, { _tag: "PreconditionFailed" });
      if (
        request.condition?._tag === "IfNoneMatch" &&
        (request.condition.etag === value.metadata.etag || request.condition.etag === "*")
      )
        return { _tag: "NotModified", metadata: value.metadata };
      return {
        _tag: "Object",
        metadata: value.metadata,
        contentLength: value.metadata.size,
        body: value.body,
      };
    }),
    delete: (request) =>
      targetIn(request).pipe(Effect.flatMap((target) => source.delete({ ...request, ...target }))),
    listPage: Effect.fnUntraced(function* (request) {
      const page = yield* source.listPage(request);
      const entries = yield* Effect.forEach(
        page.entries,
        (entry): Effect.Effect<ListEntry, StorageError> =>
          entry._tag === "CommonPrefix"
            ? Effect.succeed(entry)
            : Effect.scoped(
                decode({ ref: { ...entry.metadata.ref, storeId } }).pipe(
                  Effect.map((value) => ({ _tag: "Object" as const, metadata: value.metadata })),
                ),
              ),
      );
      return { ...page, entries };
    }),
  });
  return {
    storage,
    inspect: (target: HeadRequest) =>
      Effect.scoped(
        decode(target).pipe(
          Effect.map((value) => ({
            format: codec.format,
            logical: value.metadata,
            stored: value.stored,
          })),
        ),
      ),
  };
});

export const representationLayer = (codec: RepresentationCodec, options: RepresentationOptions) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const endpoint = yield* TransferDriver;
      const representation = yield* makeRepresentation(codec, options);
      return Context.make(Storage, representation.storage).pipe(
        Context.add(StorageRepresentation, { inspect: representation.inspect }),
        Context.add(TransferDriver, {
          storage: representation.storage,
          underlyingStoreId: endpoint.underlyingStoreId ?? endpoint.storage.storeId,
          maximumObjectBytes: options.maxDecodedBytes,
          maximumSingleUploadBytes: Math.min(
            options.maxDecodedBytes,
            endpoint.maximumSingleUploadBytes,
          ),
          transportOverheadBytes: endpoint.transportOverheadBytes,
        }),
      );
    }),
  );
