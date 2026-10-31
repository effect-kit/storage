import { Context, Effect, Layer, Predicate, Schema } from "effect";

import { StoragePublicUrls } from "../../core/capabilities.js";
import type { MultipartUploads } from "../../core/contracts/multipart.js";
import { storageError } from "../../core/errors.js";
import { ObjectRef } from "../../core/object.js";
import { Storage } from "../../core/storage.js";
import type { StorageService } from "../../core/storage.js";
import { TransferDriver } from "../../transfers/dependencies.js";
import { operationMetadataKey } from "../../transfers/upload.js";
import type { R2BucketBinding, R2BucketTransport } from "./binding.js";
import { list } from "./list.js";
import { call, validateKey } from "./mapping.js";
import { multipart } from "./multipart.js";
import { get, head, objectBytes, put, singleUploadBytes } from "./primitives.js";

export type { R2BucketBinding, R2MultipartBinding, R2ObjectLike } from "./binding.js";
export interface R2WorkersOptions {
  readonly bucket: R2BucketBinding;
  readonly storeId: string;
  readonly maxUnknownLengthBytes?: number | undefined;
}
export interface R2WorkersStorageService extends StorageService {
  readonly multipart: MultipartUploads;
}
const Options = Schema.Struct({
  bucket: Schema.declare<R2BucketBinding>(
    (input): input is R2BucketBinding =>
      Predicate.isObject(input) &&
      [
        "get",
        "head",
        "put",
        "delete",
        "list",
        "createMultipartUpload",
        "resumeMultipartUpload",
      ].every((name) => typeof Reflect.get(input, name) === "function"),
  ),
  storeId: Schema.NonEmptyString,
  maxUnknownLengthBytes: Schema.optional(
    Schema.Natural.check(Schema.isLessThanOrEqualTo(singleUploadBytes)),
  ),
});

export class R2WorkersStorage extends Context.Service<R2WorkersStorage, R2WorkersStorageService>()(
  "@effect-kit/storage/R2WorkersStorage",
) {
  static readonly make = Effect.fnUntraced(function* (options: R2WorkersOptions) {
    const config = yield* Schema.decodeUnknownEffect(Options, { onExcessProperty: "error" })(
      options,
    ).pipe(
      Effect.mapError(() =>
        storageError("make", "r2-workers", { _tag: "InvalidConfiguration", field: "options" }),
      ),
    );
    const { storeId } = config;
    const bucket = config.bucket as R2BucketTransport;
    const unknownLimit = config.maxUnknownLengthBytes ?? 8 * 1024 * 1024;
    const storage = Storage.make({
      provider: "r2-workers",
      storeId,
      capabilities: {
        putConditions: ["create-only", "if-match"],
        getConditions: ["if-match", "if-none-match"],
        deleteConditions: [],
        ranges: ["bounded", "from", "suffix"],
        versionedReads: "none",
        list: {
          maxPageSize: 1000,
          delimiter: true,
          cursorPersistence: "provider",
          snapshot: false,
        },
      },
      put: (request) => put(bucket, storeId, unknownLimit, request),
      get: (request) => get(bucket, storeId, request),
      head: (request) => head(bucket, storeId, request),
      delete: Effect.fnUntraced(function* (request) {
        const key = "ref" in request ? request.ref.key : request.key;
        yield* validateKey(key, "delete");
        yield* call("delete", () => bucket.delete(key), "unknown");
      }),
      listPage: (request) => list(bucket, storeId, request),
    });
    const service: R2WorkersStorageService = {
      ...storage,
      multipart: multipart(bucket, storeId, unknownLimit),
    };
    return service;
  });

  static layer(options: R2WorkersOptions) {
    return Layer.effectContext(
      Effect.map(R2WorkersStorage.make(options), (storage) =>
        Context.make(Storage, storage).pipe(
          Context.add(R2WorkersStorage, storage),
          Context.add(TransferDriver, {
            storage,
            multipart: storage.multipart,
            maximumObjectBytes: objectBytes,
            maximumSingleUploadBytes: singleUploadBytes,
            transportOverheadBytes: 1024 * 1024,
            // The binding has no inventory/conditional completion. No durable upload codec is supplied.
            reconcile: ({ key, operationId, size }) =>
              storage.head({ key }).pipe(
                Effect.map((saved) =>
                  saved.customMetadata?.[operationMetadataKey] === operationId &&
                  saved.size === size
                    ? saved
                    : undefined,
                ),
                Effect.catchReason("StorageError", "NotFound", () => Effect.undefined),
              ),
          }),
        ),
      ),
    );
  }

  static publicUrls(baseUrl: string) {
    return Layer.effect(
      StoragePublicUrls,
      Effect.gen(function* () {
        const storage = yield* R2WorkersStorage;
        const base = yield* Effect.try({
          try: () => new URL(baseUrl),
          catch: () =>
            storageError("publicUrls", "r2-workers", {
              _tag: "InvalidConfiguration",
              field: "baseUrl",
            }),
        });
        if (
          !["http:", "https:"].includes(base.protocol) ||
          base.username ||
          base.password ||
          base.search ||
          base.hash
        )
          return yield* storageError("publicUrls", "r2-workers", {
            _tag: "InvalidConfiguration",
            field: "baseUrl",
          });
        if (!base.pathname.endsWith("/")) base.pathname += "/";
        return {
          url: Effect.fnUntraced(function* (ref: ObjectRef) {
            const input = yield* Schema.decodeUnknownEffect(ObjectRef)(ref).pipe(
              Effect.mapError(() =>
                storageError("publicUrl", "r2-workers", { _tag: "InvalidInput", field: "ref" }),
              ),
            );
            if (input.storeId !== storage.storeId)
              return yield* storageError("publicUrl", "r2-workers", {
                _tag: "InvalidInput",
                field: "ref.storeId",
              });
            if (input.version || input.key.split("/").some((part) => part === "." || part === ".."))
              return yield* storageError("publicUrl", "r2-workers", {
                _tag: "Unsupported",
                capability: "public-url-reference",
              });
            return `${base.href}${input.key.split("/").map(encodeURIComponent).join("/")}`;
          }),
        };
      }),
    );
  }
}
