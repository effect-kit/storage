import { Effect, Redacted, Schema } from "effect";

import type { ResumableHandle } from "../core/contracts/resumable.js";
import { StorageError } from "../core/errors.js";
import { download } from "../transfers/download.js";
import { NativeHandle, nativeUpload } from "../transfers/native-upload.js";
import { planUpload } from "../transfers/planner.js";
import { sameChecksum } from "../transfers/reader.js";
import type { Operation } from "../transfers/runtime.js";
import type { UploadSource } from "../transfers/sources.js";
import { operationMetadataKey } from "../transfers/upload.js";
import type { UploadCheckpoint } from "./checkpoint.js";
import { sessionError } from "./store.js";
import type { UploadEnvironment } from "./upload.js";
import { fingerprint } from "./upload.js";

const codec = Schema.fromJsonString(Schema.toCodecJson(NativeHandle));
export const decodeNativeHandle = (value: Redacted.Redacted<string>) =>
  Schema.decodeUnknownEffect(codec)(Redacted.value(value)).pipe(
    Effect.mapError(() => sessionError("InvalidNativeHandle")),
  );

export const runNativeUpload = Effect.fnUntraced(function* <E, R>(
  environment: UploadEnvironment,
  original: UploadCheckpoint,
  source: UploadSource<E, R>,
) {
  const { runtime, store } = environment;
  if (original.state === "completed") {
    if (!original.metadata) return yield* sessionError("MissingCompletionReceipt");
    return original.metadata;
  }
  if (
    [
      "aborting",
      "cleanup-pending",
      "aborted",
      "initiating",
      "completing",
      "completion-unknown",
    ].includes(original.state)
  )
    return yield* sessionError("ReconciliationRequired", true);
  if (
    !original.protocol ||
    (original.protocol === "offset" ? !runtime.endpoint.offsets : !runtime.endpoint.blocks)
  )
    return yield* sessionError("ChangedProtocol");
  let checkpoint = original;
  const save = Effect.fnUntraced(function* (next: UploadCheckpoint) {
    const result = yield* store.write(next, checkpoint.revision);
    if (result.kind !== "upload") return yield* sessionError("InvalidCheckpoint");
    checkpoint = result;
  });
  const base = yield* runtime.begin("upload", checkpoint.length);
  base.progress.attempts = checkpoint.attempts;
  // Persist each attempt before execution, including retries and verification requests.
  const operation: Operation = {
    ...base,
    id: checkpoint.operationId,
    committed: checkpoint.state === "verifying",
    request: (effect) => {
      return Effect.gen(function* () {
        if (checkpoint.attempts >= runtime.policy.maxAttempts)
          return yield* sessionError("SessionAttemptLimit");
        yield* save({ ...checkpoint, attempts: checkpoint.attempts + 1 });
        return yield* base.request(effect);
      });
    },
  };
  const plan = yield* planUpload(
    runtime.endpoint,
    runtime.policy,
    checkpoint.length,
    runtime.input.digest.workspaceBytes * 2 + (source.workspaceBytes ?? 0),
    checkpoint.protocol,
    false,
    checkpoint.condition,
  );
  if (checkpoint.partBytes !== plan.partBytes) return yield* sessionError("ChangedTransferPolicy");
  return yield* runtime
    .deadline(
      operation,
      Effect.gen(function* () {
        if (checkpoint.state !== "verifying") {
          yield* runtime.lease(
            plan.workspaceBytes,
            Effect.scoped(
              Effect.gen(function* () {
                if (
                  source.length !== checkpoint.length ||
                  !sameChecksum(
                    yield* fingerprint(environment, source, plan.partBytes),
                    checkpoint.checksum,
                  )
                )
                  return yield* sessionError("ChangedSource");
                const handle = checkpoint.handle
                  ? yield* decodeNativeHandle(checkpoint.handle)
                  : undefined;
                if (
                  handle &&
                  (handle.ref.storeId !== runtime.endpoint.storage.storeId ||
                    handle.ref.key !== checkpoint.key)
                )
                  return yield* sessionError("ForeignHandle");
                yield* nativeUpload(
                  runtime,
                  {
                    key: checkpoint.key,
                    source,
                    expectedChecksum: checkpoint.checksum,
                    ...(checkpoint.condition ? { condition: checkpoint.condition } : {}),
                  },
                  {
                    ...checkpoint.headers,
                    customMetadata: {
                      ...checkpoint.headers.customMetadata,
                      [operationMetadataKey]: checkpoint.operationId,
                    },
                  },
                  operation,
                  plan,
                  {
                    handle,
                    blocks: checkpoint.blocks ?? [],
                    initiating:
                      checkpoint.state === "created" && checkpoint.protocol === "offset"
                        ? save({ ...checkpoint, state: "initiating" })
                        : Effect.void,
                    initiated: (upload: ResumableHandle) =>
                      Schema.encodeEffect(codec)(upload).pipe(
                        Effect.mapError(() => sessionError("InvalidNativeHandle")),
                        Effect.flatMap((encoded) =>
                          save({
                            ...checkpoint,
                            handle: Redacted.make(encoded),
                            state: "uploading",
                          }),
                        ),
                      ),
                    acknowledged: (offset, block) =>
                      save({
                        ...checkpoint,
                        state: "uploading",
                        offset,
                        ...(block
                          ? {
                              blocks: [
                                ...(checkpoint.blocks ?? []).filter(
                                  (saved) => saved.blockId !== block.blockId,
                                ),
                                block,
                              ],
                            }
                          : {}),
                      }),
                    completing: Effect.suspend(() => save({ ...checkpoint, state: "completing" })),
                    committed: (metadata) => save({ ...checkpoint, state: "verifying", metadata }),
                  },
                ).pipe(
                  Effect.catch((error) =>
                    Effect.gen(function* () {
                      if (
                        error instanceof StorageError &&
                        error.mutationOutcome === "rejected" &&
                        checkpoint.state === "completing"
                      )
                        yield* save({ ...checkpoint, state: "uploading" });
                      return yield* Effect.fail(error);
                    }),
                  ),
                );
              }),
            ),
          );
        }
        const metadata = checkpoint.metadata;
        if (!metadata) return yield* sessionError("MissingCompletionReceipt");
        operation.committed = true;
        base.committed = true;
        const verified = yield* download(
          runtime,
          {
            ref: metadata.ref,
            expectedChecksum: checkpoint.checksum,
            target: { write: () => Effect.void },
          },
          operation,
          metadata,
        );
        yield* save({
          ...checkpoint,
          state: "completed",
          metadata: verified.metadata,
          events: [
            { id: `${checkpoint.operationId}:completed`, kind: "completed", delivered: false },
          ],
        });
        return verified.metadata;
      }),
    )
    .pipe(
      Effect.mapError((error) =>
        error instanceof StorageError && operation.committed
          ? new StorageError({ ...error, mutationOutcome: "succeeded" })
          : error,
      ),
    );
});

export const abortNativeUpload = Effect.fnUntraced(function* (
  environment: UploadEnvironment,
  original: UploadCheckpoint,
) {
  if (original.state === "aborted") return;
  if (
    ["completed", "verifying", "completing", "completion-unknown", "initiating"].includes(
      original.state,
    )
  )
    return yield* sessionError("ReconciliationRequired", true);
  const { runtime, store } = environment;
  const saved = yield* store.write({ ...original, state: "aborting" }, original.revision);
  if (saved.kind !== "upload") return yield* sessionError("InvalidCheckpoint");
  if (saved.protocol === "offset" && saved.handle) {
    const handle = yield* decodeNativeHandle(saved.handle);
    if (
      handle.ref.storeId !== runtime.endpoint.storage.storeId ||
      handle.ref.key !== saved.key ||
      !runtime.endpoint.offsets
    )
      return yield* sessionError("ForeignHandle");
    const result = yield* runtime
      .cleanupRequest(runtime.endpoint.offsets.cancel(handle))
      .pipe(Effect.timeout(runtime.policy.cleanupTimeoutMillis), Effect.result);
    if (result._tag === "Failure") {
      yield* store.write({ ...saved, state: "cleanup-pending" }, saved.revision);
      return yield* sessionError("CleanupPending", true);
    }
  }
  // Azure has no block-specific cancellation. Abandon only this session's IDs; native expiry owns cleanup.
  yield* store.write(
    {
      ...saved,
      state: "aborted",
      events: [{ id: `${saved.operationId}:aborted`, kind: "aborted", delivered: false }],
    },
    saved.revision,
  );
});
