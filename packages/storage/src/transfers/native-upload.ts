import { Effect, Schema } from "effect";
import { Base64 } from "effect/encoding";

import type { ResumableHandle } from "../core/contracts/resumable.js";
import type { StorageError } from "../core/errors.js";
import { storageError } from "../core/errors.js";
import { Checksum, ObjectRef } from "../core/object.js";
import type { ObjectHeaders, ObjectMetadata } from "../core/object.js";
import type { UploadRequest } from "./models.js";
import type { UploadPlan } from "./planner.js";
import { makeReader, sameChecksum } from "./reader.js";
import type { Operation, TransferRuntime } from "./runtime.js";

export const NativeHandle = Schema.Struct({
  protocol: Schema.String,
  ref: ObjectRef,
  handle: Schema.Redacted(Schema.String),
});
export const SavedBlock = Schema.Struct({
  blockId: Schema.String,
  size: Schema.Natural,
  sourceChecksum: Checksum,
});
export type SavedBlock = typeof SavedBlock.Type;
export interface NativeJournal {
  readonly handle?: ResumableHandle | undefined;
  readonly blocks: ReadonlyArray<SavedBlock>;
  readonly initiating: Effect.Effect<void, StorageError>;
  readonly initiated: (handle: ResumableHandle) => Effect.Effect<void, StorageError>;
  readonly acknowledged: (offset: number, block?: SavedBlock) => Effect.Effect<void, StorageError>;
  readonly completing: Effect.Effect<void, StorageError>;
  readonly committed: (metadata: ObjectMetadata) => Effect.Effect<void, StorageError>;
}

/** Offset cursors and named blocks retain their own reconciliation semantics. */
export const nativeUpload = Effect.fnUntraced(function* <E, R>(
  runtime: TransferRuntime,
  request: UploadRequest<E, R>,
  headers: ObjectHeaders,
  operation: Operation,
  plan: UploadPlan,
  journal?: NativeJournal,
) {
  const { endpoint, policy } = runtime;
  const fail = (capability: string) =>
    storageError(
      "upload",
      endpoint.storage.provider,
      { _tag: "Unsupported", capability },
      "not-started",
    );
  const reader = yield* makeReader(request.source, {
    maximumBytes: plan.maximumBytes,
    maximumChunkBytes: policy.maxSourceChunkBytes,
    digest: runtime.input.digest,
    onRead: (count) => {
      operation.progress.bytesRead += count;
    },
  });
  const verify = Effect.fnUntraced(function* () {
    if ((yield* reader.next(1)) !== undefined)
      return yield* storageError(
        "upload",
        endpoint.storage.provider,
        {
          _tag: "LengthMismatch",
          expected: request.source.length ?? reader.count() - 1,
          actual: reader.count(),
        },
        "not-started",
      );
    const checksum = reader.checksum();
    if (
      !checksum ||
      (request.expectedChecksum && !sameChecksum(checksum, request.expectedChecksum))
    )
      return yield* storageError(
        "upload",
        endpoint.storage.provider,
        { _tag: "IntegrityMismatch" },
        "not-started",
      );
    return checksum;
  });
  let metadata: ObjectMetadata;
  let checksum: Checksum;
  if (plan.strategy === "offset") {
    const offsets = endpoint.offsets;
    const length = request.source.length;
    if (!offsets || length === undefined) return yield* fail("offset-known-source");
    // A zero-total status probe can also finalize an empty native upload.
    if (length === 0) yield* verify();
    if (journal) yield* journal.initiating;
    const handle =
      journal?.handle ??
      (yield* operation.request(
        offsets
          .create({
            key: request.key,
            ...headers,
            contentLength: length,
            ...(request.condition ? { condition: request.condition } : {}),
          })
          .pipe(
            Effect.catch((error) =>
              Effect.gen(function* () {
                if (error.mutationOutcome === "unknown")
                  yield* runtime.diagnostic({ kind: "unknown-outcome", operationId: operation.id });
                return yield* error;
              }),
            ),
          ),
      ));
    if (journal && !journal.handle) yield* journal.initiated(handle);
    let completionStarted = false;
    let committed = false;
    if (!journal)
      yield* Effect.addFinalizer(() =>
        committed || completionStarted
          ? Effect.void
          : runtime.cleanupRequest(offsets.cancel(handle)).pipe(
              Effect.timeout(policy.cleanupTimeoutMillis),
              Effect.result,
              Effect.flatMap((result) =>
                result._tag === "Failure"
                  ? runtime.diagnostic({ kind: "cleanup", operationId: operation.id })
                  : Effect.void,
              ),
            ),
      );
    let remote = yield* operation.request(offsets.query(handle), { retry: true });
    let position = 0;
    let pending = new Uint8Array();
    while (remote._tag === "Uploading") {
      const offset = remote.committedOffset;
      if (
        !Number.isSafeInteger(offset) ||
        offset < position ||
        offset > length ||
        (offset > position + pending.length && position !== 0)
      )
        return yield* storageError(
          "query",
          endpoint.storage.provider,
          { _tag: "InvalidSession" },
          "unknown",
        );
      if (position === 0 && pending.length === 0 && offset > 0) {
        while (reader.count() < offset) {
          const discarded = yield* reader.next(Math.min(plan.partBytes, offset - reader.count()));
          if (!discarded)
            return yield* storageError(
              "upload",
              endpoint.storage.provider,
              { _tag: "IntegrityMismatch" },
              "not-started",
            );
        }
      } else pending = pending.slice(offset - position);
      position = offset;
      if (pending.length < plan.partBytes && reader.count() < length) {
        const next = yield* reader.next(
          Math.min(plan.partBytes - pending.length, length - reader.count()),
        );
        if (!next)
          return yield* storageError(
            "upload",
            endpoint.storage.provider,
            { _tag: "LengthMismatch", expected: length, actual: reader.count() },
            "not-started",
          );
        const joined = new Uint8Array(pending.length + next.length);
        joined.set(pending);
        joined.set(next, pending.length);
        pending = joined;
      }
      const final = position + pending.length === length;
      if (final) {
        yield* verify();
        if (journal) yield* journal.completing;
        completionStarted = true;
        operation.mutationStarted = true;
      }
      const offeredEnd = position + pending.length;
      remote = yield* operation
        .request(
          Effect.suspend(() => {
            operation.progress.bytesSent += pending.length;
            return offsets.write({
              upload: handle,
              offset: position,
              body: pending,
              contentLength: pending.length,
              final,
            });
          }),
        )
        .pipe(
          Effect.catch((error) =>
            error.mutationOutcome === "unknown" || error.reason._tag === "TransportFailure"
              ? operation.request(offsets.query(handle), { retry: true })
              : Effect.fail(error),
          ),
        );
      if (
        remote._tag === "Uploading" &&
        (remote.committedOffset < position || remote.committedOffset > offeredEnd)
      )
        return yield* storageError(
          "query",
          endpoint.storage.provider,
          { _tag: "InvalidSession" },
          "unknown",
        );
      if (journal && remote._tag === "Uploading")
        yield* journal.acknowledged(remote.committedOffset);
      if (remote._tag === "Uploading")
        operation.progress.bytesAcknowledged = remote.committedOffset;
      yield* operation.observe();
    }
    if (remote._tag !== "Completed")
      return yield* storageError(
        "upload",
        endpoint.storage.provider,
        { _tag: remote._tag === "Expired" ? "Expired" : "InvalidSession" },
        "unknown",
      );
    // A lost final response can be resolved by query; source validation still precedes the receipt.
    while (reader.count() < length) {
      if (!(yield* reader.next(Math.min(plan.partBytes, length - reader.count())))) break;
    }
    checksum = yield* verify();
    metadata = offsets.resolveCompletion
      ? yield* operation.request(offsets.resolveCompletion(handle), { retry: true })
      : remote.metadata;
    committed = true;
  } else {
    const blocks = endpoint.blocks;
    if (!blocks) return yield* fail("blocks");
    const ref = { storeId: endpoint.storage.storeId, key: request.key };
    const inventory = yield* operation.request(blocks.list(ref), { retry: true });
    const observed = new Map(
      [...inventory.committed, ...inventory.uncommitted].map((block) => [
        block.blockId,
        block.size,
      ]),
    );
    const ids: string[] = [];
    while (true) {
      const body = yield* reader.next(plan.partBytes);
      if (!body) break;
      if (ids.length >= blocks.limits.maximumBlocks) return yield* fail("maximum-blocks");
      const blockId = Base64.encode(
        new TextEncoder().encode(
          operation.id.slice(0, 32).padEnd(32, "0") + ids.length.toString(16).padStart(8, "0"),
        ),
      );
      ids.push(blockId);
      const digest = runtime.input.digest.create();
      digest.update(body);
      const sourceChecksum = digest.finish();
      const saved = journal?.blocks.find((block) => block.blockId === blockId);
      if (
        !saved ||
        saved.size !== body.length ||
        !sameChecksum(saved.sourceChecksum, sourceChecksum) ||
        observed.get(blockId) !== body.length
      )
        yield* operation.request(
          Effect.suspend(() => {
            operation.progress.bytesSent += body.length;
            return blocks.stage({ ref, blockId, body, contentLength: body.length });
          }),
          { retry: true, retryUnknown: true },
        );
      if (journal)
        yield* journal.acknowledged(reader.count(), { blockId, size: body.length, sourceChecksum });
      operation.progress.bytesAcknowledged = reader.count();
      yield* operation.observe();
    }
    checksum = yield* verify();
    if (journal) yield* journal.completing;
    operation.mutationStarted = true;
    metadata = yield* operation
      .request(
        blocks.commit({
          ref,
          blockIds: ids,
          headers,
          ...(request.condition ? { condition: request.condition } : {}),
        }),
      )
      .pipe(
        Effect.catch((error) => {
          if (error.mutationOutcome !== "unknown" || !endpoint.reconcile) return Effect.fail(error);
          return operation
            .request(
              endpoint.reconcile({
                key: request.key,
                operationId: operation.id,
                size: reader.count(),
              }),
            )
            .pipe(Effect.flatMap((found) => (found ? Effect.succeed(found) : Effect.fail(error))));
        }),
      );
    metadata = { ...metadata, size: reader.count() };
  }
  if (
    metadata.ref.storeId !== endpoint.storage.storeId ||
    metadata.ref.key !== request.key ||
    (metadata.size !== undefined && metadata.size !== reader.count())
  )
    return yield* storageError(
      "upload",
      endpoint.storage.provider,
      { _tag: "IntegrityMismatch" },
      "unknown",
    );
  operation.committed = true;
  operation.progress.bytesAcknowledged = reader.count();
  operation.progress.phase = "committed";
  if (journal) yield* journal.committed(metadata);
  return { metadata, checksum, strategy: plan.strategy, progress: { ...operation.progress } };
});
