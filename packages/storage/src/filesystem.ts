import { Context, Effect, Layer } from "effect";

import type { StorageError } from "./errors.js";
import type { FilesystemOptions } from "./filesystem/layout.js";
import { makeLayout, mapIO, fileError } from "./filesystem/layout.js";
import { list } from "./filesystem/list.js";
import { get, head, put } from "./filesystem/operations.js";
import { syncDirectory } from "./runtime/filesystem.js";
import { Storage } from "./storage.js";
import type { StorageService } from "./storage.js";
import { TransferDriver } from "./transfers/dependencies.js";

export type { FilesystemOptions } from "./filesystem/layout.js";
export interface FilesystemStorageService extends StorageService {
  /** Only call after excluding every writer across processes. No age heuristic proves ownership. */
  readonly reclaimStaging: (options: {
    readonly ownership: "exclusive";
  }) => Effect.Effect<number, StorageError>;
}

export class FilesystemStorage extends Context.Service<
  FilesystemStorage,
  FilesystemStorageService
>()("@effect-kit/storage/FilesystemStorage") {
  static readonly make = Effect.fnUntraced(function* (options: FilesystemOptions) {
    const context = yield* makeLayout(options).pipe(
      Effect.mapError((error) => (error._tag === "PlatformError" ? mapIO("make")(error) : error)),
    );
    const { fs } = context;
    const storage = Storage.make({
      provider: "filesystem",
      storeId: context.storeId,
      capabilities: {
        putConditions: ["create-only"],
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
      put: (request) => put(context, request),
      get: (request) => get(context, request),
      head: (request) => head(context, request),
      delete: Effect.fnUntraced(function* (request) {
        yield* context.checkDirectories;
        const key = "ref" in request ? request.ref.key : request.key;
        const filename = yield* context.filename(key);
        yield* context.regularFile(filename).pipe(
          Effect.catchReason("PlatformError", "NotFound", () => Effect.void),
          Effect.mapError((error) =>
            error._tag === "PlatformError" ? mapIO("delete", "not-started")(error) : error,
          ),
        );
        yield* fs
          .remove(filename, { force: true })
          .pipe(
            Effect.andThen(syncDirectory(fs, context.objects)),
            Effect.uninterruptible,
            Effect.mapError(mapIO("delete", "unknown")),
          );
      }),
      listPage: (request) => list(context, request),
    });
    const service: FilesystemStorageService = {
      ...storage,
      reclaimStaging: Effect.fnUntraced(function* (ownership) {
        if (ownership.ownership !== "exclusive")
          return yield* fileError("reclaimStaging", "ExclusiveOwnershipRequired", "not-started");
        yield* context.checkDirectories;
        const names = yield* fs
          .readDirectory(context.staging)
          .pipe(Effect.mapError(mapIO("reclaimStaging")));
        let removed = 0;
        for (const name of names) {
          if (!/^upload-[a-f0-9-]{36}\.pending$/u.test(name)) continue;
          const filename = context.path.join(context.staging, name);
          yield* context
            .regularFile(filename)
            .pipe(
              Effect.mapError((error) =>
                error._tag === "PlatformError" ? mapIO("reclaimStaging")(error) : error,
              ),
            );
          yield* fs
            .remove(filename)
            .pipe(Effect.uninterruptible, Effect.mapError(mapIO("reclaimStaging", "unknown")));
          removed++;
        }
        yield* syncDirectory(fs, context.staging).pipe(
          Effect.uninterruptible,
          Effect.mapError(mapIO("reclaimStaging", "unknown")),
        );
        return removed;
      }),
    };
    return service;
  });

  static layer(options: FilesystemOptions) {
    return Layer.effectContext(
      Effect.map(FilesystemStorage.make(options), (service) =>
        Context.make(Storage, service).pipe(
          Context.add(FilesystemStorage, service),
          Context.add(TransferDriver, {
            storage: service,
            maximumObjectBytes: options.maxObjectBytes ?? 1024 * 1024 * 1024,
            maximumSingleUploadBytes: options.maxObjectBytes ?? 1024 * 1024 * 1024,
            transportOverheadBytes: 65536 + 32768,
          }),
        ),
      ),
    );
  }
}
