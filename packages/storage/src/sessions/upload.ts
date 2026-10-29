import { Effect } from "effect";

import { StorageError } from "../errors.js";
import { download } from "../transfers/download.js";
import { partReceipt } from "../transfers/part-receipts.js";
import { planUpload } from "../transfers/planner.js";
import { makeReader, sameChecksum } from "../transfers/reader.js";
import type { TransferRuntime } from "../transfers/runtime.js";
import type { UploadSource } from "../transfers/sources.js";
import { operationMetadataKey } from "../transfers/upload.js";
import type { UploadCheckpoint } from "./checkpoint.js";
import type { UploadSessionStore } from "./store.js";
import { sessionError } from "./store.js";

export interface SessionOptions {
  readonly configurationId: string;
  readonly expiresInMillis?: number;
}
export interface UploadEnvironment {
  readonly runtime: TransferRuntime;
  readonly store: UploadSessionStore["Service"];
  readonly options: SessionOptions;
}

export const fingerprint = Effect.fnUntraced(function* <E, R>(
  environment: UploadEnvironment,
  source: UploadSource<E, R>,
  partBytes: number,
) {
  if (!source.replayable || source.length === undefined)
    return yield* sessionError("ReplayableKnownSourceRequired");
  const { runtime } = environment;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const reader = yield* makeReader(source, {
        maximumBytes: runtime.policy.maxBytes,
        maximumChunkBytes: runtime.policy.maxSourceChunkBytes,
        digest: runtime.input.digest,
        onRead: () => {},
      });
      while ((yield* reader.next(partBytes)) !== undefined) {
        /* Hash in source order. */
      }
      const checksum = reader.checksum();
      if (!checksum) return yield* sessionError("FingerprintMissing");
      return checksum;
    }),
  );
});

export const runUpload = Effect.fnUntraced(function* <E, R>(
  environment: UploadEnvironment,
  original: UploadCheckpoint,
  source: UploadSource<E, R>,
) {
  const { runtime, store } = environment;
  if (original.state === "completed") {
    if (!original.metadata) return yield* sessionError("MissingCompletionReceipt");
    return original.metadata;
  }
  const codec = runtime.endpoint.handles;
  if (!codec) return yield* sessionError("DurableUploadUnsupported");
  const multipart = runtime.endpoint.multipart;
  if (!multipart) return yield* sessionError("MultipartRequired");
  let checkpoint = original;
  const operation = yield* runtime.begin("upload", checkpoint.length);
  operation.progress.attempts = checkpoint.attempts;
  operation.committed = checkpoint.state === "verifying";
  const plan = yield* planUpload(
    runtime.endpoint,
    runtime.policy,
    checkpoint.length,
    runtime.input.digest.workspaceBytes * 2 + (source.workspaceBytes ?? 0),
    "multipart",
  );
  if (plan.partBytes !== checkpoint.partBytes) return yield* sessionError("ChangedTransferPolicy");
  const save = Effect.fnUntraced(function* (next: UploadCheckpoint) {
    const saved = yield* store.write(next, checkpoint.revision);
    if (saved.kind !== "upload") return yield* sessionError("InvalidCheckpoint");
    checkpoint = saved;
  });
  const call = <A>(effect: Effect.Effect<A, StorageError>) =>
    Effect.gen(function* () {
      if (checkpoint.attempts >= runtime.policy.maxAttempts)
        return yield* sessionError("SessionAttemptLimit");
      yield* save({ ...checkpoint, attempts: checkpoint.attempts + 1 });
      return yield* operation.request(effect);
    });
  const verification = {
    ...operation,
    request: <A, EX = never, RX = never>(effect: Effect.Effect<A, StorageError | EX, RX>) =>
      Effect.gen(function* () {
        if (checkpoint.attempts >= runtime.policy.maxAttempts)
          return yield* sessionError("SessionAttemptLimit");
        yield* save({ ...checkpoint, attempts: checkpoint.attempts + 1 });
        return yield* operation.request(effect);
      }),
  };
  const work = runtime.lease(
    plan.workspaceBytes,
    Effect.scoped(
      Effect.gen(function* () {
        if (["aborting", "cleanup-pending", "aborted"].includes(checkpoint.state))
          return yield* sessionError("SessionAborting");
        if (["initiating", "completing", "completion-unknown"].includes(checkpoint.state))
          return yield* sessionError("ReconciliationRequired", true);

        if (
          checkpoint.state !== "verifying" &&
          (source.length !== checkpoint.length ||
            !sameChecksum(
              yield* fingerprint(environment, source, checkpoint.partBytes),
              checkpoint.checksum,
            ))
        )
          return yield* sessionError("ChangedSource");
        if (checkpoint.state === "created") {
          yield* save({ ...checkpoint, state: "initiating" });
          const handle = yield* call(
            multipart.initiate({
              key: checkpoint.key,
              headers: {
                ...checkpoint.headers,
                customMetadata: {
                  ...checkpoint.headers.customMetadata,
                  [operationMetadataKey]: checkpoint.operationId,
                },
              },
            }),
          );
          yield* save({
            ...checkpoint,
            handle: yield* codec.encode(handle),
            state: "uploading",
          });
        }
        if (!checkpoint.handle) return yield* sessionError("MissingHandle");
        const handle = yield* codec.decode(checkpoint.handle);
        if (
          handle.ref.storeId !== runtime.endpoint.storage.storeId ||
          handle.ref.key !== checkpoint.key
        )
          return yield* sessionError("ForeignHandle");
        if (checkpoint.state === "uploading") {
          const observed = new Map<number, { etag: string; size?: number | undefined }>();
          const cursors = new Set<string>();
          let cursor: string | undefined;
          do {
            const page = yield* call(
              multipart.listParts({ upload: handle, ...(cursor ? { cursor } : {}) }),
            );
            for (const part of page.parts) {
              if (observed.has(part.partNumber) || observed.size >= multipart.limits.maximumParts)
                return yield* sessionError("InvalidPartInventory");
              observed.set(part.partNumber, part);
            }
            cursor = page.cursor;
            if (cursor && cursors.has(cursor)) return yield* sessionError("RepeatedPartCursor");
            if (cursor) cursors.add(cursor);
          } while (cursor);
          const reader = yield* makeReader(source, {
            maximumBytes: runtime.policy.maxBytes,
            maximumChunkBytes: runtime.policy.maxSourceChunkBytes,
            digest: runtime.input.digest,
            onRead: () => {},
          });
          let number = 0;
          while (true) {
            const chunk = yield* reader.next(checkpoint.partBytes);
            if (!chunk && number > 0) break;
            const body = chunk ?? new Uint8Array();
            number++;
            const saved = checkpoint.parts.find((part) => part.partNumber === number);
            const remote = observed.get(number);
            const partHash = runtime.input.digest.create();
            partHash.update(body);
            const sourceChecksum = partHash.finish();
            if (
              !saved ||
              !sameChecksum(sourceChecksum, saved.sourceChecksum) ||
              remote?.etag !== saved.etag ||
              remote.size !== saved.size
            ) {
              const receipt = yield* partReceipt(
                yield* call(
                  multipart.uploadPart({
                    upload: handle,
                    partNumber: number,
                    body,
                    contentLength: body.length,
                  }),
                ),
                number,
                body.length,
                runtime.endpoint.storage.provider,
              );
              yield* save({
                ...checkpoint,
                parts: [
                  ...checkpoint.parts.filter((part) => part.partNumber !== number),
                  { ...receipt, sourceChecksum },
                ].toSorted((a, b) => a.partNumber - b.partNumber),
              });
            }
            if (!chunk) break;
          }
          const checksum = reader.checksum();
          if (!checksum || !sameChecksum(checksum, checkpoint.checksum))
            return yield* sessionError("ChangedSource");
          if (checkpoint.attempts + 3 > runtime.policy.maxAttempts)
            return yield* sessionError("SessionAttemptLimit");
          yield* save({ ...checkpoint, state: "completing" });
          const metadata = yield* call(
            Effect.suspend(() => {
              operation.mutationStarted = true;
              return multipart.complete({
                upload: handle,
                parts: checkpoint.parts.map(({ sourceChecksum: _sourceChecksum, ...part }) => part),
                ...(checkpoint.condition ? { condition: checkpoint.condition } : {}),
              });
            }),
          ).pipe(
            Effect.catch((error) =>
              Effect.gen(function* () {
                if (error.mutationOutcome === "rejected") {
                  operation.mutationStarted = false;
                  yield* save({ ...checkpoint, state: "uploading" }).pipe(Effect.result);
                }
                return yield* error;
              }),
            ),
          );
          operation.committed = true;
          yield* save({ ...checkpoint, metadata, state: "verifying" });
        }
        if (!checkpoint.metadata) return yield* sessionError("MissingCompletionReceipt");
        return checkpoint.metadata;
      }),
    ),
  );
  return yield* runtime
    .deadline(
      verification,
      Effect.gen(function* () {
        const metadata = yield* work;
        if (!metadata) return yield* sessionError("MissingCompletionReceipt");
        if (checkpoint.state !== "completed") {
          const verified = yield* download(
            runtime,
            {
              ref: metadata.ref,
              expectedChecksum: checkpoint.checksum,
              target: { write: () => Effect.void },
            },
            operation,
          );
          yield* save({
            ...checkpoint,
            state: "completed",
            metadata: verified.metadata,
            events: [
              { id: `${checkpoint.operationId}:completed`, kind: "completed", delivered: false },
            ],
          });
        }
        return checkpoint.metadata ?? metadata;
      }),
    )
    .pipe(
      Effect.mapError((error) =>
        error instanceof StorageError && operation.committed
          ? new StorageError({
              operation: error.operation,
              provider: error.provider,
              reason: error.reason,
              requestId: error.requestId,
              mutationOutcome: "succeeded",
            })
          : error,
      ),
    );
});

export const reconcileUpload = Effect.fnUntraced(function* (
  environment: UploadEnvironment,
  original: UploadCheckpoint,
) {
  const { runtime, store } = environment;
  if (!["initiating", "completing", "completion-unknown"].includes(original.state)) return original;
  // An unknown initiation has no safely observed handle. Never adopt an upload by key alone.
  if (original.state === "initiating" || !runtime.endpoint.reconcile)
    return yield* sessionError("OrphanDiscoveryRequired", true);
  const metadata = yield* runtime
    .cleanupRequest(
      runtime.endpoint.reconcile({
        key: original.key,
        operationId: original.operationId,
        size: original.length,
      }),
    )
    .pipe(
      Effect.timeoutOrElse({
        duration: runtime.policy.cleanupTimeoutMillis,
        orElse: () => Effect.fail(sessionError("ReconciliationTimeout", true)),
      }),
    );
  const next = metadata
    ? { ...original, state: "verifying" as const, metadata }
    : { ...original, state: "completion-unknown" as const };
  return yield* store.write(next, original.revision);
});

export const abortUpload = Effect.fnUntraced(function* (
  environment: UploadEnvironment,
  original: UploadCheckpoint,
) {
  const { runtime, store } = environment;
  if (original.state === "aborted") return;
  if (
    ["completed", "verifying", "completing", "completion-unknown", "initiating"].includes(
      original.state,
    )
  )
    return yield* sessionError("ReconciliationRequired", true);
  let checkpoint = yield* store.write({ ...original, state: "aborting" }, original.revision);
  if (checkpoint.kind !== "upload") return yield* sessionError("InvalidCheckpoint");
  if (checkpoint.handle) {
    const codec = runtime.endpoint.handles;
    if (!codec) return yield* sessionError("DurableUploadUnsupported");
    const multipart = runtime.endpoint.multipart;
    if (!multipart) return yield* sessionError("MultipartRequired");
    const handle = yield* codec.decode(checkpoint.handle);
    if (
      handle.ref.storeId !== runtime.endpoint.storage.storeId ||
      handle.ref.key !== checkpoint.key
    )
      return yield* sessionError("ForeignHandle");
    if (original.state === "aborting" || original.state === "cleanup-pending") {
      const inventory = yield* runtime
        .cleanupRequest(multipart.listParts({ upload: handle }))
        .pipe(Effect.timeout(runtime.policy.cleanupTimeoutMillis), Effect.result);
      if (
        inventory._tag === "Failure" &&
        inventory.failure._tag === "StorageError" &&
        inventory.failure.reason._tag === "InvalidSession"
      ) {
        yield* store.write(
          {
            ...checkpoint,
            state: "aborted",
            events: [
              { id: `${checkpoint.operationId}:aborted`, kind: "aborted", delivered: false },
            ],
          },
          checkpoint.revision,
        );
        return;
      }
    }
    const result = yield* runtime
      .cleanupRequest(multipart.abort(handle))
      .pipe(Effect.timeout(runtime.policy.cleanupTimeoutMillis), Effect.result);
    if (result._tag === "Failure") {
      yield* store.write({ ...checkpoint, state: "cleanup-pending" }, checkpoint.revision);
      return yield* result.failure._tag === "StorageError"
        ? result.failure
        : sessionError("CleanupPending", true);
    }
  }
  checkpoint = yield* store.write(
    {
      ...checkpoint,
      state: "aborted",
      events: [{ id: `${checkpoint.operationId}:aborted`, kind: "aborted", delivered: false }],
    },
    checkpoint.revision,
  );
});
