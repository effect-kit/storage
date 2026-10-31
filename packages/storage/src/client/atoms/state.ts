import type { Cause } from "effect";

import type { UploadError, UploadStatus } from "../../uploads/schema.js";
import type { UploadProgress } from "../transport.js";

export type UploadPhase =
  | "idle"
  | "hashing"
  | "authorizing"
  | "uploading"
  | "verifying"
  | "publishing"
  | "completed"
  | "paused"
  | "cancelling"
  | "cancelled"
  | "failed"
  | "uncertain"
  | "disposed";

export interface UploadSnapshot {
  readonly phase: UploadPhase;
  readonly operation: number;
  readonly uploadId?: string;
  readonly resumeId?: string;
  readonly progress?: UploadProgress;
  readonly status?: UploadStatus;
  readonly failure?: Cause.Cause<UploadError>;
  readonly canPause: boolean;
  readonly canResume: boolean;
  readonly isRunning: boolean;
}

export const idleSnapshot: UploadSnapshot = Object.freeze({
  phase: "idle",
  operation: 0,
  canPause: false,
  canResume: false,
  isRunning: false,
});
