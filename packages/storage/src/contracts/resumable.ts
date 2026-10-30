import type { Effect, Redacted } from "effect";

import type { StorageError } from "../errors.js";
import type { ObjectHeaders, ObjectMetadata, ObjectRef } from "../object.js";
import type { PutRequest, UploadBody, WriteCondition } from "../requests.js";

export interface ResumableHandle {
  readonly protocol: string;
  readonly ref: ObjectRef;
  readonly handle: Redacted.Redacted<string>;
}

export type ResumableState =
  | { readonly _tag: "Uploading"; readonly committedOffset: number }
  | { readonly _tag: "Completed"; readonly metadata: ObjectMetadata }
  | { readonly _tag: "Expired" }
  | { readonly _tag: "Unknown" };

/** Offset protocols such as GCS/tus, with provider-specific constraints. */
export interface OffsetUploads {
  readonly limits: { readonly alignmentBytes: number; readonly maximumChunkBytes: number };
  readonly create: (
    request: Omit<PutRequest, "body">,
  ) => Effect.Effect<ResumableHandle, StorageError>;
  readonly query: (upload: ResumableHandle) => Effect.Effect<ResumableState, StorageError>;
  readonly write: <E = never, R = never>(request: {
    readonly upload: ResumableHandle;
    readonly offset: number;
    readonly body: UploadBody<E, R>;
    readonly contentLength: number;
    readonly final: boolean;
  }) => Effect.Effect<ResumableState, StorageError | E, R>;
  readonly cancel: (upload: ResumableHandle) => Effect.Effect<void, StorageError>;
}

/** Azure-style named blocks are not reported as byte offsets or S3 parts. */
export interface BlockUploads {
  readonly limits: { readonly maximumBlockBytes: number; readonly maximumBlocks: number };
  readonly stage: <E = never, R = never>(request: {
    readonly ref: ObjectRef;
    readonly blockId: string;
    readonly body: UploadBody<E, R>;
    readonly contentLength: number;
  }) => Effect.Effect<void, StorageError | E, R>;
  readonly list: (ref: ObjectRef) => Effect.Effect<
    {
      readonly committed: ReadonlyArray<{ readonly blockId: string; readonly size: number }>;
      readonly uncommitted: ReadonlyArray<{ readonly blockId: string; readonly size: number }>;
    },
    StorageError
  >;
  readonly commit: (request: {
    readonly ref: ObjectRef;
    readonly blockIds: ReadonlyArray<string>;
    readonly headers?: ObjectHeaders;
    readonly condition?: WriteCondition;
  }) => Effect.Effect<ObjectMetadata, StorageError>;
}
