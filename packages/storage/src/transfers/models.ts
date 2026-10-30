import type { Effect, Result, Stream } from "effect";

import type {
  MultipartHandle,
  MultipartHandleCodec,
  MultipartUploads,
  UploadedPart,
} from "../contracts/multipart.js";
import type { BlockUploads, OffsetUploads } from "../contracts/resumable.js";
import type { StorageError } from "../errors.js";
import type { Checksum, ObjectHeaders, ObjectMetadata, ObjectRef } from "../object.js";
import type { WriteCondition } from "../requests.js";
import type { StorageService } from "../storage.js";
import type { UploadSource } from "./sources.js";

/** Implementations must hash incrementally, retaining no input buffers. */
export interface TransferDigest {
  readonly algorithm: string;
  readonly workspaceBytes: number;
  readonly create: () => {
    readonly update: (bytes: Uint8Array) => void;
    readonly finish: () => Checksum;
    readonly snapshot?: () => Checksum;
  };
}

/** A provider supplies protocol operations, never transfer orchestration or retries. */
export interface TransferEndpoint {
  /** Original backend identity retained by facades to reject false cross-store isolation. */
  readonly underlyingStoreId?: string;
  readonly storage: StorageService;
  readonly maximumObjectBytes: number;
  readonly maximumSingleUploadBytes: number;
  readonly transportOverheadBytes: number;
  readonly validateUpload?:
    | ((request: {
        readonly key: string;
        readonly headers: ObjectHeaders;
        readonly condition?: WriteCondition | undefined;
      }) => Effect.Effect<void, StorageError>)
    | undefined;
  readonly offsets?: OffsetUploads | undefined;
  readonly blocks?: BlockUploads | undefined;
  readonly multipart?: MultipartUploads | undefined;
  readonly handles?: MultipartHandleCodec | undefined;
  readonly nativeCopy?:
    | {
        readonly maximumBytes: number;
        readonly accepts: (source: TransferEndpoint) => boolean;
        readonly copy: (request: CopyIntent) => Effect.Effect<ObjectMetadata, StorageError>;
        readonly part?: (
          request: CopyIntent & {
            readonly upload: MultipartHandle;
            readonly partNumber: number;
            readonly start: number;
            readonly end: number;
          },
        ) => Effect.Effect<UploadedPart, StorageError>;
      }
    | undefined;
  /** Return a receipt only when it proves this exact operation committed. */
  readonly reconcile?: (intent: {
    readonly key: string;
    readonly operationId: string;
    readonly size: number;
  }) => Effect.Effect<ObjectMetadata | undefined, StorageError>;
}

export interface CopyIntent {
  readonly source: TransferEndpoint;
  readonly ref: ObjectRef;
  readonly etag?: string | undefined;
  readonly destinationKey: string;
  readonly headers: ObjectHeaders;
  readonly condition?: WriteCondition | undefined;
}

export interface TransferProgress {
  readonly operation: "upload" | "download" | "copy" | "move";
  readonly phase: "running" | "committed" | "verified";
  readonly totalBytes?: number | undefined;
  readonly bytesRead: number;
  /** Payload offered to protocol calls, including retries, rather than socket-level counts. */
  readonly bytesSent: number;
  readonly bytesAcknowledged: number;
  readonly bytesVerified: number;
  readonly attempts: number;
}

export interface TransferPolicy {
  readonly memoryBudgetBytes?: number;
  readonly partBytes?: number;
  readonly partConcurrency?: number;
  readonly requestConcurrency?: number;
  readonly maxSourceChunkBytes?: number;
  readonly maxBytes?: number;
  readonly deadlineMillis?: number;
  /** Total protocol requests per logical operation, including HEADs and retries. */
  readonly maxAttempts?: number;
  readonly retryBaseMillis?: number;
  readonly retryMaxMillis?: number;
  readonly cleanupTimeoutMillis?: number;
  readonly observerTimeoutMillis?: number;
  readonly observer?: (progress: TransferProgress) => Effect.Effect<void, unknown>;
}

export type TransferConfiguration = TransferPolicy & { readonly digest: TransferDigest };

export interface UploadRequest<E = never, R = never> {
  readonly key: string;
  readonly source: UploadSource<E, R>;
  readonly headers?: ObjectHeaders;
  readonly condition?: WriteCondition;
  readonly expectedChecksum?: Checksum;
  readonly strategy?: "auto" | "single" | "multipart" | "offset" | "blocks";
}

export interface DownloadTarget<E = never, R = never> {
  /** Completion acknowledges that the entire chunk has been accepted, in order. */
  readonly write: (bytes: Uint8Array) => Effect.Effect<void, E, R>;
}

export interface DownloadRequest<E = never, R = never> {
  readonly key?: string;
  readonly ref?: ObjectRef;
  readonly target: DownloadTarget<E, R>;
  readonly expectedChecksum?: Checksum;
}

export interface CopyRequest {
  readonly source: TransferEndpoint;
  readonly ref: ObjectRef;
  readonly destinationKey: string;
  readonly condition?: WriteCondition;
}

export interface TransferReceipt {
  readonly metadata: ObjectMetadata;
  readonly strategy:
    | "single"
    | "multipart"
    | "offset"
    | "blocks"
    | "download"
    | "native-copy"
    | "multipart-copy"
    | "streamed-copy";
  readonly checksum: Checksum;
  readonly progress: TransferProgress;
}

export type MoveReceipt =
  | { readonly _tag: "Moved"; readonly destination: TransferReceipt }
  | {
      readonly _tag: "Copied";
      readonly destination: TransferReceipt;
      readonly deleteError: StorageError;
    };

/** Safe bounded diagnostics. Upload handles remain redacted and are never metric labels. */
export interface TransferDiagnostic {
  readonly kind: "observer" | "cleanup" | "unknown-outcome";
  readonly operationId: string;
  readonly upload?: MultipartHandle | undefined;
  readonly error?: StorageError | undefined;
}

export interface TransfersService {
  readonly endpoint: TransferEndpoint;
  readonly upload: <E = never, R = never>(
    request: UploadRequest<E, R>,
  ) => Effect.Effect<TransferReceipt, StorageError | E, R>;
  readonly download: <E = never, R = never>(
    request: DownloadRequest<E, R>,
  ) => Effect.Effect<TransferReceipt, StorageError | E, R>;
  readonly copy: (request: CopyRequest) => Effect.Effect<TransferReceipt, StorageError>;
  readonly move: (request: CopyRequest) => Effect.Effect<MoveReceipt, StorageError>;
  readonly uploadMany: <E, R, EX, RX>(
    requests: Stream.Stream<UploadRequest<E, R>, EX, RX>,
    concurrency?: number,
  ) => Stream.Stream<Result.Result<TransferReceipt, StorageError | E>, EX | StorageError, R | RX>;
  readonly downloadMany: <E, R, EX, RX>(
    requests: Stream.Stream<DownloadRequest<E, R>, EX, RX>,
    concurrency?: number,
  ) => Stream.Stream<Result.Result<TransferReceipt, StorageError | E>, EX | StorageError, R | RX>;
  readonly diagnostics: Effect.Effect<{
    readonly entries: ReadonlyArray<TransferDiagnostic>;
    readonly dropped: number;
  }>;
  readonly resources: Effect.Effect<{
    readonly reservedBytes: number;
    readonly peakReservedBytes: number;
    readonly activeRequests: number;
    readonly peakActiveRequests: number;
  }>;
}
