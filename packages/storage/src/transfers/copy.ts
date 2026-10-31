import type { Cause } from "effect";
import { Clock, Effect, Exit, Queue, Stream } from "effect";

import type { StorageError } from "../core/errors.js";
import { storageError } from "../core/errors.js";
import { ObjectHeaders } from "../core/object.js";
import type { Checksum, ObjectMetadata } from "../core/object.js";
import { decodeRequest } from "../internal/request-validation.js";
import { download, pinnedRequest } from "./download.js";
import type { CopyIntent, CopyRequest, MoveReceipt, TransferReceipt } from "./models.js";
import { partReceipt } from "./part-receipts.js";
import { planUpload } from "./planner.js";
import type { Operation, TransferRuntime } from "./runtime.js";
import { stream } from "./sources.js";
import {
  cleanupMultipart,
  initiateMultipart,
  operationMetadataKey,
  reconcile,
  upload,
  validateUpload,
} from "./upload.js";

const quietStage = (operation: Operation): Operation => ({
  ...operation,
  progress: {
    ...operation.progress,
    bytesRead: 0,
    bytesSent: 0,
    bytesAcknowledged: 0,
    bytesVerified: 0,
  },
  observe: () => Effect.void,
});

const copy = Effect.fnUntraced(function* (
  runtime: TransferRuntime,
  request: CopyRequest,
  operation: Operation,
  selected?: ObjectMetadata,
): Effect.fn.Return<TransferReceipt, StorageError> {
  const source = request.source;
  const destination = runtime.endpoint;
  const provider = destination.storage.provider;
  if (
    source.storage.storeId === destination.storage.storeId &&
    request.ref.key === request.destinationKey
  )
    return yield* storageError(
      "copy",
      provider,
      { _tag: "InvalidInput", field: "destinationKey" },
      "not-started",
    );
  const metadata =
    selected ??
    (yield* operation.request(source.storage.head({ ref: request.ref }), { retry: true }));
  const pinned = pinnedRequest(source, metadata);
  if (!pinned || metadata.size === undefined)
    return yield* storageError(
      "copy",
      provider,
      { _tag: "Unsupported", capability: "pinned-sized-copy" },
      "not-started",
    );
  operation.progress.totalBytes = metadata.size;

  const {
    ref,
    size,
    etag,
    lastModified: _modified,
    checksums: _checksums,
    storageClass: _class,
    ...headers
  } = metadata;
  const overrides = request.headers
    ? yield* decodeRequest(ObjectHeaders, request.headers, "copy", provider)
    : {};
  if (overrides.customMetadata?.[operationMetadataKey] !== undefined)
    return yield* storageError(
      "copy",
      provider,
      { _tag: "InvalidInput", field: `headers.customMetadata.${operationMetadataKey}` },
      "not-started",
    );
  const cleanHeaders = {
    ...headers,
    ...overrides,
    customMetadata: { ...headers.customMetadata, ...overrides.customMetadata },
  };
  delete cleanHeaders.customMetadata[operationMetadataKey];
  yield* validateUpload(runtime, {
    key: request.destinationKey,
    source: stream(Stream.empty, size),
    headers: cleanHeaders,
    ...(request.condition ? { condition: request.condition } : {}),
  });
  if (size > Math.min(runtime.policy.maxBytes, destination.maximumObjectBytes))
    return yield* storageError(
      "copy",
      provider,
      {
        _tag: "LimitExceeded",
        limit: Math.min(runtime.policy.maxBytes, destination.maximumObjectBytes),
      },
      "not-started",
    );
  const intent: CopyIntent = {
    source,
    ref: "ref" in pinned ? pinned.ref : ref,
    etag,
    destinationKey: request.destinationKey,
    condition: request.condition,
    headers: {
      ...cleanHeaders,
      customMetadata: { ...cleanHeaders.customMetadata, [operationMetadataKey]: operation.id },
    },
  };
  const sourceRuntime = { ...runtime, endpoint: source };
  const native = destination.nativeCopy?.accepts(source) ? destination.nativeCopy : undefined;
  let receipt: TransferReceipt;
  let expected: Checksum;

  if (native && (size <= native.maximumBytes || (native.part && destination.multipart))) {
    // Native copy receipts are not full-object digests. Hash the pinned source first.
    const sourceReceipt = yield* download(
      sourceRuntime,
      { ref: intent.ref, target: { write: () => Effect.void } },
      quietStage(operation),
      metadata,
    );
    expected = sourceReceipt.checksum;
    operation.progress.bytesRead = size;
    let copied: ObjectMetadata;
    let strategy: TransferReceipt["strategy"] = "native-copy";
    if (size <= native.maximumBytes) {
      copied = yield* operation
        .request(
          Effect.suspend(() => {
            operation.mutationStarted = true;
            return native.copy(intent);
          }),
          { retry: true },
        )
        .pipe(
          Effect.catch((error) =>
            reconcile(runtime, operation, request.destinationKey, size, error),
          ),
        );
    } else {
      strategy = "multipart-copy";
      const multipart = destination.multipart;
      const copyPart = native.part;
      if (!multipart || !copyPart)
        return yield* storageError(
          "copy",
          provider,
          { _tag: "Unsupported", capability: "multipart-copy" },
          "not-started",
        );
      const plan = yield* planUpload(
        destination,
        runtime.policy,
        size,
        runtime.input.digest.workspaceBytes,
        "multipart",
        true,
        request.condition,
      );
      copied = yield* runtime.lease(
        plan.workspaceBytes,
        Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* initiateMultipart(
              runtime,
              operation,
              multipart,
              request.destinationKey,
              intent.headers,
            );
            let completed = false;
            let unknown = false;
            yield* Effect.addFinalizer(() =>
              completed
                ? Effect.void
                : unknown
                  ? runtime.diagnostic({
                      kind: "unknown-outcome",
                      operationId: operation.id,
                      upload: handle,
                    })
                  : cleanupMultipart(runtime, operation, handle, multipart),
            );
            const count = Math.ceil(size / plan.partBytes);
            const parts = yield* Effect.forEach(
              Array.from({ length: count }, (_, index) => index),
              (index) =>
                operation
                  .request(
                    copyPart({
                      ...intent,
                      upload: handle,
                      partNumber: index + 1,
                      start: index * plan.partBytes,
                      end: Math.min(size, (index + 1) * plan.partBytes) - 1,
                    }),
                    { retry: true, retryUnknown: true },
                  )
                  .pipe(
                    Effect.flatMap((part) =>
                      partReceipt(
                        part,
                        index + 1,
                        Math.min(plan.partBytes, size - index * plan.partBytes),
                        provider,
                      ),
                    ),
                  ),
              { concurrency: plan.concurrency },
            );
            const result = yield* operation
              .request(
                Effect.suspend(() => {
                  unknown = true;
                  operation.mutationStarted = true;
                  return multipart.complete({
                    upload: handle,
                    parts,
                    ...(request.condition ? { condition: request.condition } : {}),
                  });
                }),
                { retry: true },
              )
              .pipe(
                Effect.catch((error) =>
                  Effect.gen(function* () {
                    unknown = error.mutationOutcome === "unknown";
                    return yield* reconcile(
                      runtime,
                      operation,
                      request.destinationKey,
                      size,
                      error,
                    );
                  }),
                ),
              );
            completed = true;
            return result;
          }),
        ),
      );
    }
    operation.progress.bytesAcknowledged = size;
    receipt = {
      metadata: copied,
      checksum: expected,
      strategy,
      progress: { ...operation.progress },
    };
  } else {
    const plan = yield* planUpload(
      destination,
      runtime.policy,
      size,
      runtime.input.digest.workspaceBytes,
      "auto",
      false,
      request.condition,
    );
    const sourceWorkspace =
      runtime.policy.maxSourceChunkBytes * 2 +
      source.transportOverheadBytes +
      runtime.input.digest.workspaceBytes;
    if (
      runtime.policy.requestConcurrency < 2 ||
      plan.workspaceBytes + sourceWorkspace > runtime.policy.memoryBudgetBytes
    )
      return yield* storageError(
        "copy",
        provider,
        { _tag: "QuotaExceeded", limit: runtime.policy.memoryBudgetBytes },
        "not-started",
      );
    let sourceChecksum: Checksum | undefined;
    receipt = yield* runtime.lease(
      plan.workspaceBytes + sourceWorkspace,
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* Queue.bounded<Uint8Array, StorageError | Cause.Done>(0);
          const producer = download(
            sourceRuntime,
            {
              ref: intent.ref,
              target: { write: (bytes) => Queue.offer(queue, bytes).pipe(Effect.asVoid) },
            },
            quietStage(operation),
            metadata,
            true,
          ).pipe(
            Effect.tap((result) =>
              Effect.sync(() => {
                sourceChecksum = result.checksum;
              }),
            ),
            Effect.onExit((exit) =>
              Exit.isSuccess(exit) ? Queue.end(queue) : Queue.failCause(queue, exit.cause),
            ),
          );
          yield* Effect.forkScoped(producer);
          const result = yield* upload(
            runtime,
            {
              key: request.destinationKey,
              source: stream(Stream.fromQueue(queue), size),
              headers: cleanHeaders,
              ...(request.condition ? { condition: request.condition } : {}),
            },
            operation,
            true,
          );
          return { ...result, strategy: "streamed-copy" as const };
        }),
      ),
    );
    if (!sourceChecksum)
      return yield* storageError("copy", provider, { _tag: "IntegrityMismatch" }, "succeeded");
    expected = sourceChecksum;
  }

  operation.committed = true;
  // Verify the exact returned destination version before declaring copy success or deleting source.
  const verify = yield* download(
    runtime,
    { ref: receipt.metadata.ref, target: { write: () => Effect.void }, expectedChecksum: expected },
    quietStage(operation),
  ).pipe(
    Effect.mapError((error) => storageError("copy.verify", provider, error.reason, "succeeded")),
  );
  operation.progress.bytesVerified = verify.progress.bytesVerified;
  operation.progress.phase = "verified";
  return { ...receipt, metadata: verify.metadata, progress: { ...operation.progress } };
});

const runCopyOrMove = Effect.fnUntraced(function* (
  runtime: TransferRuntime,
  request: CopyRequest,
  move: boolean,
): Effect.fn.Return<TransferReceipt | MoveReceipt, StorageError> {
  const operation = yield* runtime.begin(move ? "move" : "copy");
  const prepare = Effect.gen(function* () {
    // Select deletion authority before copying; a mutable key is never sufficient for a move.
    const original = yield* operation.request(request.source.storage.head({ ref: request.ref }), {
      retry: true,
    });
    const history =
      request.source.storage.capabilities.versionedReads !== "none" && original.ref.version;
    const conditional =
      original.etag &&
      !original.etag.startsWith("W/") &&
      request.source.storage.capabilities.deleteConditions.includes("if-match")
        ? original.etag
        : undefined;
    if (move && !history && !conditional)
      return yield* storageError(
        "move",
        runtime.endpoint.storage.provider,
        { _tag: "Unsupported", capability: "pinned-source-delete" },
        "not-started",
      );
    const destination = yield* copy(
      runtime,
      { ...request, ref: original.ref },
      operation,
      original,
    );
    return { original, conditional, destination };
  });
  const { original, conditional, destination } = yield* runtime.deadline(operation, prepare);
  if (!move) {
    yield* operation.observe("verified");
    return destination;
  }

  const now = yield* Clock.currentTimeMillis;
  const remaining = Math.max(0, operation.startedAt + runtime.policy.deadlineMillis - now);
  let deleteStarted = false;
  const result = yield* operation
    .request(
      Effect.suspend(() => {
        deleteStarted = true;
        return request.source.storage.delete({
          ref: original.ref,
          ...(conditional ? { condition: { _tag: "IfMatch" as const, etag: conditional } } : {}),
        });
      }),
    )
    .pipe(
      Effect.timeoutOrElse({
        duration: remaining,
        orElse: () =>
          Effect.fail(
            storageError(
              "move.delete",
              request.source.storage.provider,
              { _tag: "TransportFailure", code: "TransferDeadlineExceeded" },
              deleteStarted ? "unknown" : "not-started",
            ),
          ),
      }),
      Effect.result,
    );
  yield* operation.observe("verified");
  return result._tag === "Success"
    ? { _tag: "Moved" as const, destination }
    : { _tag: "Copied" as const, destination, deleteError: result.failure };
});

export function copyOrMove(
  runtime: TransferRuntime,
  request: CopyRequest,
  move: false,
): Effect.Effect<TransferReceipt, StorageError>;
export function copyOrMove(
  runtime: TransferRuntime,
  request: CopyRequest,
  move: true,
): Effect.Effect<MoveReceipt, StorageError>;
export function copyOrMove(runtime: TransferRuntime, request: CopyRequest, move: boolean) {
  return runCopyOrMove(runtime, request, move).pipe(
    Effect.withSpan(move ? "Transfers.move" : "Transfers.copy"),
  );
}
