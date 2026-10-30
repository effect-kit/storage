import type { Effect } from "effect";

import type { StorageError } from "../errors.js";
import type { ObjectMetadata } from "../object.js";
import type { UploadActor, UploadInput, UploadRecord } from "./schema.js";
import type { failUpload } from "./schema.js";
export type UploadAction =
  | "create"
  | "grant"
  | "status"
  | "complete"
  | "abort"
  | "reconcile"
  | "content";
export interface UploadDecision {
  readonly key: string;
  readonly maxBytes: number;
  readonly allowedContentTypes: ReadonlyArray<string>;
  readonly expiresInSeconds: number;
  readonly context: Readonly<Record<string, string>>;
}
export interface UploadOptions<R = never> {
  /** Both staging and unpublished final objects must be inaccessible to application readers. */
  readonly quarantine: "private";
  readonly grantSeconds?: number;
  readonly authorize: (input: {
    readonly actor: UploadActor;
    readonly input: UploadInput;
    readonly uploadId: string;
    readonly action: UploadAction;
  }) => Effect.Effect<UploadDecision, ReturnType<typeof failUpload>, R>;
  /** Must be idempotent using the supplied key in the application's transaction/outbox. */
  readonly publish: (input: {
    readonly idempotencyKey: string;
    readonly object: ObjectMetadata;
    readonly context: Readonly<Record<string, string>>;
  }) => Effect.Effect<unknown, ReturnType<typeof failUpload>, R>;
  readonly inspect?: (input: {
    readonly object: ObjectMetadata;
    readonly file: UploadInput["file"];
  }) => Effect.Effect<void, ReturnType<typeof failUpload> | StorageError, R>;
  readonly quota?: {
    readonly reserve: (
      input: UploadRecord,
    ) => Effect.Effect<void, ReturnType<typeof failUpload>, R>;
    readonly settle: (input: UploadRecord) => Effect.Effect<void, ReturnType<typeof failUpload>, R>;
    readonly release: (
      input: UploadRecord,
    ) => Effect.Effect<void, ReturnType<typeof failUpload>, R>;
  };
}
