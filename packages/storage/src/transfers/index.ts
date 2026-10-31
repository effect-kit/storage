import { Context, Effect, FileSystem, Layer, Path, Stream } from "effect";

import { storageError } from "../core/errors.js";
import type { Checksum, ObjectRef } from "../core/object.js";
import { copyOrMove } from "./copy.js";
import { download } from "./download.js";
import { ManagedTransferRuntime, makeManagedRuntime } from "./managed-runtime.js";
import type {
  CopyRequest,
  DownloadRequest,
  TransferPolicy,
  TransfersService,
  UploadRequest,
} from "./models.js";
import type { TransferRuntime } from "./runtime.js";
import { upload } from "./upload.js";

export { TransferDriver, TransferHash } from "./dependencies.js";
export type { MultipartHandleCodec } from "../core/contracts/multipart.js";

export * as UploadSource from "./sources.js";
export type {
  TransferDigest,
  TransferEndpoint,
  CopyIntent,
  TransferProgress,
  TransferPolicy,
  UploadRequest,
  DownloadTarget,
  DownloadRequest,
  CopyRequest,
  TransferReceipt,
  MoveReceipt,
  TransferDiagnostic,
  TransfersService,
} from "./models.js";
export type { UploadSource as UploadSourceType } from "./sources.js";

export class Transfers extends Context.Service<Transfers, TransfersService>()(
  "@effect-kit/storage/Transfers",
) {
  static readonly make = Effect.fnUntraced(function* (policy: TransferPolicy = {}) {
    return Transfers.fromRuntime(yield* makeManagedRuntime(policy));
  });

  private static fromRuntime(runtime: TransferRuntime) {
    const { endpoint, policy } = runtime;
    const many = <A, B, E, R, EX, RX>(
      requests: Stream.Stream<A, EX, RX>,
      run: (request: A) => Effect.Effect<B, E, R>,
      concurrency: number,
    ) => {
      if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 1024)
        return Stream.fail(
          storageError(
            "transfers.many",
            endpoint.storage.provider,
            { _tag: "InvalidInput", field: "concurrency" },
            "not-started",
          ),
        );
      return requests.pipe(
        Stream.mapEffect((request) => Effect.result(run(request)), { concurrency }),
      );
    };
    return Transfers.of({
      endpoint,
      upload: (request) => upload(runtime, request),
      download: (request) => download(runtime, request),
      copy: (request) => copyOrMove(runtime, request, false),
      move: (request) => copyOrMove(runtime, request, true),
      uploadMany: (requests, concurrency = policy.partConcurrency) =>
        many(requests, (request) => upload(runtime, request), concurrency),
      downloadMany: (requests, concurrency = policy.partConcurrency) =>
        many(requests, (request) => download(runtime, request), concurrency),
      diagnostics: runtime.diagnostics,
      resources: runtime.resources,
    });
  }

  static layer(policy: TransferPolicy = {}) {
    return Layer.effectContext(
      Effect.map(makeManagedRuntime(policy), (runtime) =>
        Context.make(Transfers, Transfers.fromRuntime(runtime)).pipe(
          Context.add(ManagedTransferRuntime, runtime),
        ),
      ),
    );
  }

  static upload<E, R>(request: UploadRequest<E, R>) {
    return Effect.flatMap(Transfers, (transfers) => transfers.upload(request));
  }

  static download<E, R>(request: DownloadRequest<E, R>) {
    return Effect.flatMap(Transfers, (transfers) => transfers.download(request));
  }

  static copy(request: CopyRequest) {
    return Effect.flatMap(Transfers, (transfers) => transfers.copy(request));
  }

  static move(request: CopyRequest) {
    return Effect.flatMap(Transfers, (transfers) => transfers.move(request));
  }

  static uploadMany<E, R, EX, RX>(
    requests: Stream.Stream<UploadRequest<E, R>, EX, RX>,
    concurrency?: number,
  ) {
    return Stream.unwrap(
      Effect.map(Transfers, (transfers) => transfers.uploadMany(requests, concurrency)),
    );
  }

  static downloadMany<E, R, EX, RX>(
    requests: Stream.Stream<DownloadRequest<E, R>, EX, RX>,
    concurrency?: number,
  ) {
    return Stream.unwrap(
      Effect.map(Transfers, (transfers) => transfers.downloadMany(requests, concurrency)),
    );
  }

  /** Stages beside the destination, closes the file, then atomically renames after verification. */
  static readonly downloadFile = Effect.fn("Transfers.downloadFile")(function* (request: {
    readonly path: string;
    readonly key?: string;
    readonly ref?: ObjectRef;
    readonly expectedChecksum?: Checksum;
  }) {
    const transfers = yield* Transfers;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const temporary = yield* fs.makeTempFileScoped({
          directory: path.dirname(request.path),
          prefix: ".effect-storage-",
        });
        const receipt = yield* Effect.scoped(
          Effect.gen(function* () {
            const file = yield* fs.open(temporary, { flag: "w" });
            return yield* transfers.download({
              ...(request.ref ? { ref: request.ref } : { key: request.key ?? "" }),
              ...(request.expectedChecksum ? { expectedChecksum: request.expectedChecksum } : {}),
              target: { write: (bytes) => file.writeAll(bytes) },
            });
          }),
        );
        yield* fs.rename(temporary, request.path);
        return receipt;
      }),
    );
  });
}
