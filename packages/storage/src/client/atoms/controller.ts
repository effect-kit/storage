import { Cause, Effect, Exit, Fiber, Semaphore } from "effect";

import { UploadClient } from "../../client.js";
import type { UploadProtocol } from "../../client.js";
import { failUpload } from "../../uploads/schema.js";
import type { UploadError, UploadInput, UploadStatus } from "../../uploads/schema.js";
import type { UploadTicket } from "../store.js";
import type { UploadProgress } from "../transport.js";
import { idleSnapshot } from "./state.js";
import type { UploadSnapshot } from "./state.js";

export interface UploadSlotOptions {
  readonly purpose: string;
  readonly context: Readonly<Record<string, string>>;
  readonly strategy?: UploadInput["strategy"];
}

/** One slot per registry. The owning scope joins work on disposal, independently of subscribers. */
export const makeController = Effect.fnUntraced(function* (
  options: UploadSlotOptions,
  publish: (snapshot: UploadSnapshot) => void,
) {
  const client = yield* UploadClient;
  const scope = yield* Effect.scope;
  const lock = yield* Semaphore.make(1);
  let snapshot = idleSnapshot;
  let active: Fiber.Fiber<void, UploadError> | undefined;
  let file: File | undefined;
  let ticket: UploadTicket | undefined;
  let recoveryId: string | undefined;
  let closed = false;
  let generation = 0;
  let progressTimer: ReturnType<typeof setTimeout> | undefined;
  let pendingProgress: UploadProgress | undefined;
  const clearProgress = () => {
    if (progressTimer !== undefined) clearTimeout(progressTimer);
    progressTimer = undefined;
    pendingProgress = undefined;
  };

  const update = (patch: Partial<UploadSnapshot>) => {
    snapshot = Object.freeze({ ...snapshot, ...patch });
    publish(snapshot);
  };
  const ensureOpen = () => (closed ? Effect.fail(failUpload("Conflict")) : Effect.void);
  const stop = Effect.suspend(() => {
    generation++;
    clearProgress();
    const previous = active;
    active = undefined;
    return previous ? Fiber.interrupt(previous) : Effect.void;
  });
  let resumable = false;
  const progress = (operation: number) => (value: UploadProgress) => {
    if (operation !== generation || closed) return;
    pendingProgress = Object.freeze({ ...value });
    if (progressTimer !== undefined) return;
    progressTimer = setTimeout(() => {
      const latest = pendingProgress;
      clearProgress();
      if (!latest || operation !== generation || closed || !snapshot.isRunning) return;
      // Fingerprinting also reports completed; only server publication completes an upload.
      const phase = latest.phase === "completed" ? snapshot.phase : latest.phase;
      update({ progress: latest, phase, canPause: phase === "uploading" && resumable });
    }, 16);
  };
  const finish = (status: UploadStatus) => {
    const latestProgress = pendingProgress;
    clearProgress();
    update({
      ...(latestProgress ? { progress: latestProgress } : {}),
      phase:
        status.state === "published"
          ? "completed"
          : status.state === "publishing"
            ? "publishing"
            : status.state === "verifying"
              ? "verifying"
              : status.state === "aborted"
                ? "cancelled"
                : status.state === "rejected"
                  ? "failed"
                  : "uncertain",
      status,
      uploadId: status.id,
      isRunning: false,
      canPause: false,
      canResume: !["published", "aborted", "rejected"].includes(status.state),
    });
    if (["published", "aborted", "rejected"].includes(status.state)) file = undefined;
  };
  const launch = Effect.fnUntraced(function* (
    selected: File,
    resume: boolean,
    suppliedId?: string,
  ) {
    yield* ensureOpen();
    if (snapshot.isRunning) return yield* failUpload("Conflict");
    if (resume && !suppliedId && !recoveryId) return yield* failUpload("NotFound");
    if (!resume && !["idle", "completed", "cancelled"].includes(snapshot.phase))
      return yield* failUpload("Conflict");
    const previousId =
      ticket?.id ??
      snapshot.uploadId ??
      (suppliedId && /^[a-f0-9]{64}$/u.test(suppliedId) ? suppliedId : undefined);
    const operation = ++generation;
    resumable = false;
    file = selected;
    if (!resume) {
      ticket = undefined;
      recoveryId = crypto.randomUUID();
    } else if (suppliedId) recoveryId = suppliedId;
    const checkpoint = recoveryId;
    if (!checkpoint) return yield* failUpload("NotFound");
    snapshot = Object.freeze({
      ...idleSnapshot,
      operation,
      phase: "hashing",
      isRunning: true,
      ...(recoveryId ? { resumeId: recoveryId } : {}),
      ...(resume && previousId ? { uploadId: previousId } : {}),
    });
    publish(snapshot);
    const onProgress = progress(operation);
    const onProtocol = (protocol: UploadProtocol, uploadId: string) => {
      if (operation !== generation || closed) return;
      clearProgress();
      resumable = protocol === "multipart" || protocol === "resumable";
      recoveryId = uploadId;
      update({ phase: "uploading", uploadId, resumeId: uploadId, canPause: resumable });
    };
    const work = Effect.gen(function* () {
      let result: UploadStatus;
      if (resume) {
        result = yield* client.resume({
          uploadId: checkpoint,
          file: selected,
          onProgress,
          onProtocol,
        });
      } else {
        ticket = yield* client.create({
          file: selected,
          purpose: options.purpose,
          context: options.context,
          idempotencyKey: checkpoint,
          onProgress,
          ...(options.strategy ? { strategy: options.strategy } : {}),
        });
        if (operation !== generation) return;
        const id = ticket.id;
        if (!id) return yield* failUpload("Unavailable");
        recoveryId = id;
        update({ phase: "uploading", uploadId: id, resumeId: id, canPause: resumable });
        const sent = yield* client.transfer({
          uploadId: id,
          file: selected,
          onProgress,
          onProtocol,
        });
        const transferred = pendingProgress;
        clearProgress();
        update({
          phase: "verifying",
          canPause: false,
          ...(transferred ? { progress: transferred } : {}),
        });
        result = yield* client.complete({ ...sent, onProgress });
      }
      if (operation === generation) finish(result);
    }).pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => {
          if (operation !== generation || closed) return;
          active = undefined;
          if (Exit.isFailure(exit)) {
            clearProgress();
            update({
              phase: Cause.hasInterrupts(exit.cause) ? "paused" : "uncertain",
              failure: exit.cause,
              isRunning: false,
              canPause: false,
              canResume: true,
              ...(ticket?.id ? { uploadId: ticket.id } : {}),
            });
          }
        }),
      ),
    );
    active = yield* Effect.forkIn(work, scope);
  });
  const serialize = <A, E>(effect: Effect.Effect<A, E>) => lock.withPermit(effect);
  const close = (notify: boolean) =>
    serialize(
      Effect.gen(function* () {
        if (closed) return;
        closed = true;
        yield* stop;
        file = undefined;
        if (notify)
          update({ phase: "disposed", isRunning: false, canPause: false, canResume: false });
      }),
    );
  const dispose = close(true);
  // Registry teardown has already invalidated its nodes; finalization must not republish.
  yield* Effect.addFinalizer(() => close(false));

  return {
    start: (selected: File) => serialize(launch(selected, false)),
    resume: (input: { readonly file: File; readonly uploadId?: string }) =>
      serialize(launch(input.file, true, input.uploadId)),
    retry: serialize(
      Effect.suspend(() => (file ? launch(file, true) : Effect.fail(failUpload("NotFound")))),
    ),
    pause: serialize(
      Effect.gen(function* () {
        yield* ensureOpen();
        if (!snapshot.canPause) return yield* failUpload("Unsupported");
        yield* stop;
        update({ phase: "paused", isRunning: false, canPause: false, canResume: true });
      }),
    ),
    cancel: serialize(
      Effect.gen(function* () {
        yield* ensureOpen();
        if (snapshot.phase === "completed") return yield* failUpload("Conflict");
        yield* stop;
        update({ phase: "cancelling", isRunning: true, canPause: false });
        const uploadId = ticket?.id ?? snapshot.uploadId;
        if (!uploadId) {
          update({
            phase: recoveryId ? "uncertain" : "cancelled",
            isRunning: false,
            canResume: Boolean(recoveryId),
          });
          return;
        }
        yield* client.abort({ uploadId }).pipe(
          Effect.tap((status) => Effect.sync(() => finish(status))),
          Effect.onExit((exit) =>
            Effect.sync(() => {
              if (Exit.isFailure(exit))
                update({
                  phase: "uncertain",
                  failure: exit.cause,
                  isRunning: false,
                  canResume: true,
                });
            }),
          ),
        );
      }),
    ),
    reconcile: serialize(
      Effect.gen(function* () {
        yield* ensureOpen();
        if (snapshot.isRunning) return yield* failUpload("Conflict");
        if (!snapshot.uploadId) return yield* failUpload("NotFound");
        const status = yield* client.reconcile({ uploadId: snapshot.uploadId });
        finish(status);
        return status;
      }),
    ),
    reset: serialize(
      Effect.gen(function* () {
        yield* ensureOpen();
        if (snapshot.isRunning) return yield* failUpload("Conflict");
        generation++;
        file = undefined;
        ticket = undefined;
        recoveryId = undefined;
        snapshot = Object.freeze({ ...idleSnapshot, operation: generation });
        publish(snapshot);
      }),
    ),
    dispose,
  };
});
