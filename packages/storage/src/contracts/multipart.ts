import type { Effect, Redacted } from "effect";

import type { StorageError } from "../errors.js";
import type { Checksum, ObjectHeaders, ObjectMetadata, ObjectRef } from "../object.js";
import type { ListRequest, UploadBody, WriteCondition } from "../requests.js";

export interface MultipartHandle {
  readonly ref: ObjectRef;
  readonly uploadId: Redacted.Redacted<string>;
}

export interface UploadedPart {
  readonly partNumber: number;
  readonly etag: string;
  readonly size?: number;
  readonly checksums?: ReadonlyArray<Checksum>;
}

/** S3-style numbered parts; block/offset protocols have separate contracts. */
export interface MultipartUploads {
  readonly limits: {
    readonly minimumPartBytes: number;
    readonly maximumPartBytes: number;
    readonly maximumParts: number;
  };
  readonly initiate: (request: {
    readonly key: string;
    readonly headers?: ObjectHeaders;
  }) => Effect.Effect<MultipartHandle, StorageError>;
  readonly uploadPart: <E = never, R = never>(request: {
    readonly upload: MultipartHandle;
    readonly partNumber: number;
    readonly body: UploadBody<E, R>;
    readonly contentLength?: number;
  }) => Effect.Effect<UploadedPart, StorageError | E, R>;
  readonly listParts: (request: {
    readonly upload: MultipartHandle;
    readonly cursor?: string;
  }) => Effect.Effect<
    { readonly parts: ReadonlyArray<UploadedPart>; readonly cursor?: string },
    StorageError
  >;
  readonly complete: (request: {
    readonly upload: MultipartHandle;
    readonly parts: ReadonlyArray<UploadedPart>;
    readonly condition?: WriteCondition;
  }) => Effect.Effect<ObjectMetadata, StorageError>;
  readonly abort: (upload: MultipartHandle) => Effect.Effect<void, StorageError>;
  readonly listUploads?: (
    request: ListRequest,
  ) => Effect.Effect<
    { readonly uploads: ReadonlyArray<MultipartHandle>; readonly cursor?: string },
    StorageError
  >;
}
