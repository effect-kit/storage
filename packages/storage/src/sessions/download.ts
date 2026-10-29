import { Effect, Stream } from "effect";

import { StorageError } from "../errors.js";
import type { Checksum } from "../object.js";
import { pinnedRequest } from "../transfers/download.js";
import { sameChecksum } from "../transfers/reader.js";
import type { DownloadCheckpoint } from "./checkpoint.js";
import { sessionError } from "./store.js";
import type { UploadEnvironment } from "./upload.js";

export interface RestartableDownloadTarget<E = never, R = never> {
  readonly id: string;
  readonly identity?: string;
  readonly discard?: () => Effect.Effect<void, E, R>;
  readonly prefix: (length: number) => Stream.Stream<Uint8Array, E, R>;
  /** Discard uncheckpointed tail bytes; never extend a short existing prefix. */
  readonly truncate: (length: number) => Effect.Effect<void, E, R>;
  readonly write: (offset: number, bytes: Uint8Array) => Effect.Effect<void, E, R>;
  readonly sync: Effect.Effect<void, E, R>;
  /** Must verify or idempotently recognize a prior publication of these exact bytes. */
  readonly publish: (checksum: Checksum, length: number) => Effect.Effect<void, E, R>;
}

export const runDownload = Effect.fnUntraced(function* <E, R>(
  environment: UploadEnvironment,
  original: DownloadCheckpoint,
  target: RestartableDownloadTarget<E, R>,
) {
  const { runtime, store } = environment;
  if (target.id !== original.targetId || target.id !== original.id)
    return yield* sessionError("ForeignDownloadTarget");
  let checkpoint = original;
  const operation = yield* runtime.begin("download", checkpoint.length);
  const workspace =
    runtime.policy.maxSourceChunkBytes * 2 +
    runtime.input.digest.workspaceBytes * 2 +
    runtime.endpoint.transportOverheadBytes;
  if (workspace > runtime.policy.memoryBudgetBytes)
    return yield* sessionError("DownloadWorkspaceLimit");
  const save = Effect.fnUntraced(function* (next: DownloadCheckpoint) {
    const saved = yield* store.write(next, checkpoint.revision);
    if (saved.kind !== "download") return yield* sessionError("InvalidCheckpoint");
    checkpoint = saved;
  });
  const work = runtime.lease(
    workspace,
    Effect.scoped(
      Effect.gen(function* () {
        const identity = target.identity ?? target.id;
        if (checkpoint.targetIdentity && checkpoint.targetIdentity !== identity)
          return yield* sessionError("ChangedDownloadTarget");
        if (!checkpoint.targetIdentity) yield* save({ ...checkpoint, targetIdentity: identity });
        if (["aborting", "aborted"].includes(checkpoint.state))
          return yield* sessionError("SessionAborting");
        if (checkpoint.state === "completed") return checkpoint.metadata;
        if (checkpoint.state === "verified") {
          if (!checkpoint.checksum) return yield* sessionError("MissingVerifiedDigest");
          operation.mutationStarted = true;
          yield* target.publish(checkpoint.checksum, checkpoint.length);
          operation.committed = true;
          yield* save({ ...checkpoint, state: "completed" });
          return checkpoint.metadata;
        }
        const pin = pinnedRequest(runtime.endpoint, checkpoint.metadata);
        if (!pin) return yield* sessionError("PinnedDownloadRequired");
        // A crash/error after the last write may precede EOF/native checksum validation.
        if (checkpoint.offset === checkpoint.length && checkpoint.offset > 0)
          yield* save({ ...checkpoint, offset: 0, prefixChecksum: undefined });
        yield* target.truncate(checkpoint.offset);
        const hash = runtime.input.digest.create();
        const prefixHash = runtime.input.digest.create();
        const snapshot = hash.snapshot?.bind(hash);
        if (!snapshot) return yield* sessionError("DigestSnapshotRequired");
        let prefixBytes = 0;
        yield* Stream.runForEach(target.prefix(checkpoint.offset), (chunk) =>
          Effect.gen(function* () {
            if (
              chunk.buffer.byteLength > runtime.policy.maxSourceChunkBytes ||
              prefixBytes + chunk.length > checkpoint.offset
            )
              return yield* sessionError("InvalidDownloadPrefix");
            hash.update(chunk);
            prefixHash.update(chunk);
            prefixBytes += chunk.length;
          }),
        );
        if (
          prefixBytes !== checkpoint.offset ||
          (checkpoint.offset > 0 &&
            (!checkpoint.prefixChecksum ||
              !sameChecksum(prefixHash.finish(), checkpoint.prefixChecksum)))
        )
          return yield* sessionError("ChangedDownloadPrefix");
        const start = checkpoint.offset;
        yield* operation.request(
          Effect.scoped(
            Effect.gen(function* () {
              const response = yield* runtime.endpoint.storage.get({
                ...pin,
                ...(start > 0 ? { range: { _tag: "From" as const, start } } : {}),
              });
              if (
                response._tag !== "Object" ||
                (checkpoint.metadata.etag && checkpoint.metadata.etag !== response.metadata.etag)
              )
                return yield* sessionError("ChangedDownloadSource");
              if (
                start > 0 &&
                (response.range?.start !== start || response.range.totalSize !== checkpoint.length)
              )
                return yield* sessionError("InvalidDownloadRange");
              yield* Stream.runForEach(response.body, (chunk) =>
                Effect.gen(function* () {
                  if (
                    chunk.buffer.byteLength > runtime.policy.maxSourceChunkBytes ||
                    checkpoint.offset + chunk.length > checkpoint.length
                  )
                    return yield* sessionError("DownloadLengthMismatch");
                  hash.update(chunk);
                  yield* target.write(checkpoint.offset, chunk);
                  yield* target.sync;
                  yield* save({
                    ...checkpoint,
                    offset: checkpoint.offset + chunk.length,
                    prefixChecksum: snapshot(),
                  });
                }),
              );
              if (
                checkpoint.offset !== checkpoint.length ||
                (response.contentLength !== undefined &&
                  response.contentLength !== checkpoint.offset - start)
              )
                return yield* sessionError("DownloadLengthMismatch");
            }),
          ),
        );
        const checksum = hash.finish();
        const expected =
          checkpoint.expectedChecksum ??
          checkpoint.metadata.checksums?.find(
            (candidate) =>
              candidate.scope === "full-object" && candidate.algorithm === checksum.algorithm,
          );
        if (expected && !sameChecksum(checksum, expected))
          return yield* sessionError("DownloadChecksumMismatch");
        yield* save({ ...checkpoint, state: "verified", checksum });
        operation.mutationStarted = true;
        yield* target.publish(checksum, checkpoint.length);
        operation.committed = true;
        yield* save({ ...checkpoint, state: "completed" });
        return checkpoint.metadata;
      }),
    ),
  );
  return yield* runtime.deadline(operation, work).pipe(
    Effect.mapError((error) =>
      error instanceof StorageError &&
      operation.mutationStarted &&
      error.mutationOutcome !== "rejected"
        ? new StorageError({
            operation: error.operation,
            provider: error.provider,
            reason: error.reason,
            requestId: error.requestId,
            mutationOutcome: operation.committed ? "succeeded" : "unknown",
          })
        : error,
    ),
  );
});
