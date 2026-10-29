import { Cause, Effect, Exit, Option, Stream } from "effect";

import type { MultipartHandle, MultipartUploads } from "../contracts/multipart.js";
import { StorageError, storageError } from "../errors.js";
import { decodeRequest } from "../internal/request-validation.js";
import { Checksum, ObjectHeaders, ObjectKey } from "../object.js";
import type { ObjectMetadata } from "../object.js";
import { WriteCondition } from "../requests.js";
import type { TransferReceipt, UploadRequest } from "./models.js";
import { partReceipt } from "./part-receipts.js";
import { planUpload } from "./planner.js";
import { makeReader, sameChecksum } from "./reader.js";
import type { Operation, TransferRuntime } from "./runtime.js";

export const operationMetadataKey = "effect-storage-operation";

export const validateUpload = Effect.fnUntraced(function* <E, R>(
  runtime: TransferRuntime,
  request: UploadRequest<E, R>,
) {
  const { provider } = runtime.endpoint.storage;
  if (
    !request.source ||
    typeof request.source.open !== "function" ||
    (request.source.workspaceBytes !== undefined &&
      (!Number.isSafeInteger(request.source.workspaceBytes) || request.source.workspaceBytes < 0))
  )
    return yield* storageError(
      "upload",
      provider,
      { _tag: "InvalidInput", field: "source" },
      "not-started",
    );
  yield* decodeRequest(ObjectKey, request.key, "upload", provider, true);
  const headers = yield* decodeRequest(
    ObjectHeaders,
    request.headers ?? {},
    "upload",
    provider,
    true,
  );
  if (request.condition)
    yield* decodeRequest(WriteCondition, request.condition, "upload", provider, true);
  if (request.expectedChecksum)
    yield* decodeRequest(Checksum, request.expectedChecksum, "upload", provider, true);
  if (headers.customMetadata?.[operationMetadataKey] !== undefined)
    return yield* storageError(
      "upload",
      provider,
      { _tag: "InvalidInput", field: `headers.customMetadata.${operationMetadataKey}` },
      "not-started",
    );
  if (
    request.expectedChecksum &&
    (request.expectedChecksum.scope !== "full-object" ||
      request.expectedChecksum.algorithm !== runtime.input.digest.algorithm)
  )
    return yield* storageError(
      "upload",
      provider,
      { _tag: "Unsupported", capability: "expected-full-object-checksum" },
      "not-started",
    );
  const condition = request.condition?._tag === "CreateOnly" ? "create-only" : "if-match";
  if (request.condition && !runtime.endpoint.storage.capabilities.putConditions.includes(condition))
    return yield* storageError(
      "upload",
      provider,
      { _tag: "Unsupported", capability: condition },
      "not-started",
    );
  return headers;
});

export const initiateMultipart = Effect.fnUntraced(function* (
  runtime: TransferRuntime,
  operation: Operation,
  multipart: MultipartUploads,
  key: string,
  headers: ObjectHeaders,
) {
  let started = false;
  return yield* operation
    .request(
      Effect.suspend(() => {
        started = true;
        return multipart.initiate({ key, headers });
      }),
      { retry: true },
    )
    .pipe(
      Effect.onExit((exit) => {
        if (!started || Exit.isSuccess(exit)) return Effect.void;
        const error = Option.getOrUndefined(Cause.findErrorOption(exit.cause));
        return error instanceof StorageError && error.mutationOutcome !== "unknown"
          ? Effect.void
          : runtime.diagnostic({ kind: "unknown-outcome", operationId: operation.id });
      }),
    );
});

export const reconcile = Effect.fnUntraced(function* (
  runtime: TransferRuntime,
  operation: Operation,
  key: string,
  size: number,
  error: StorageError,
) {
  if (error.mutationOutcome !== "unknown") return yield* error;
  if (runtime.endpoint.reconcile) {
    const result = yield* operation
      .request(runtime.endpoint.reconcile({ key, operationId: operation.id, size }))
      .pipe(Effect.result);
    if (result._tag === "Success" && result.success) return result.success;
  }
  yield* runtime.diagnostic({ kind: "unknown-outcome", operationId: operation.id, error });
  return yield* error;
});

/** Finalizers run after mapEffect has joined its scoped producer and part workers. */
export function cleanupMultipart(
  runtime: TransferRuntime,
  operation: Operation,
  upload: MultipartHandle,
  multipart: MultipartUploads,
) {
  return Effect.gen(function* () {
    const result = yield* Effect.exit(
      runtime
        .cleanupRequest(multipart.abort(upload))
        .pipe(Effect.interruptible, Effect.timeout(runtime.policy.cleanupTimeoutMillis)),
    );
    if (Exit.isFailure(result)) {
      const error = Option.getOrUndefined(Cause.findErrorOption(result.cause));
      yield* runtime.diagnostic({
        kind: "cleanup",
        operationId: operation.id,
        upload,
        error: error?._tag === "StorageError" ? error : undefined,
      });
    }
  });
}

export const upload = Effect.fn("Transfers.upload")(function* <E, R>(
  runtime: TransferRuntime,
  request: UploadRequest<E, R>,
  existing?: Operation,
  reserved = false,
): Effect.fn.Return<TransferReceipt, StorageError | E, R> {
  const headers = yield* validateUpload(runtime, request);
  const plan = yield* planUpload(
    runtime.endpoint,
    runtime.policy,
    request.source.length,
    runtime.input.digest.workspaceBytes + (request.source.workspaceBytes ?? 0),
    request.strategy,
  );
  const operation = existing ?? (yield* runtime.begin("upload", request.source.length));
  const { endpoint, policy } = runtime;
  const intentHeaders = {
    ...headers,
    customMetadata: { ...headers.customMetadata, [operationMetadataKey]: operation.id },
  };

  const work = Effect.scoped(
    Effect.gen(function* () {
      const reader = yield* makeReader(request.source, {
        maximumBytes: plan.maximumBytes,
        maximumChunkBytes: policy.maxSourceChunkBytes,
        digest: runtime.input.digest,
        onRead: (count) => {
          operation.progress.bytesRead += count;
        },
      });
      const verify = () =>
        Effect.gen(function* () {
          const checksum = reader.checksum();
          if (!checksum)
            return yield* storageError(
              "upload",
              endpoint.storage.provider,
              { _tag: "IntegrityMismatch" },
              "not-started",
            );
          if (request.expectedChecksum && !sameChecksum(checksum, request.expectedChecksum))
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
      if (plan.strategy === "single") {
        const body = (yield* reader.next(plan.partBytes)) ?? new Uint8Array();
        // Read EOF before publication, including exact-sized and empty objects.
        if ((yield* reader.next(1)) !== undefined)
          return yield* storageError(
            "upload",
            endpoint.storage.provider,
            { _tag: "LimitExceeded", limit: plan.partBytes },
            "not-started",
          );
        checksum = yield* verify();
        const put = Effect.suspend(() => {
          operation.mutationStarted = true;
          operation.progress.bytesSent += body.length;
          return endpoint.storage.put({
            key: request.key,
            ...intentHeaders,
            condition: request.condition,
            contentLength: body.length,
            body,
          });
        });
        metadata = yield* operation
          .request(put, { retry: true })
          .pipe(
            Effect.catch((error) => reconcile(runtime, operation, request.key, body.length, error)),
          );
        operation.progress.bytesAcknowledged = body.length;
      } else {
        const multipart = endpoint.multipart;
        if (!multipart)
          return yield* storageError(
            "upload",
            endpoint.storage.provider,
            { _tag: "Unsupported", capability: "multipart" },
            "not-started",
          );
        const handle = yield* initiateMultipart(
          runtime,
          operation,
          multipart,
          request.key,
          intentHeaders,
        );
        let completed = false;
        let unknownCompletion = false;
        yield* Effect.addFinalizer(() =>
          completed
            ? Effect.void
            : unknownCompletion
              ? runtime.diagnostic({
                  kind: "unknown-outcome",
                  operationId: operation.id,
                  upload: handle,
                })
              : cleanupMultipart(runtime, operation, handle, multipart),
        );
        let partNumber = 0;
        const parts = yield* Stream.fromPull(
          Effect.succeed(
            Effect.gen(function* () {
              const body = yield* reader.next(plan.partBytes);
              if (!body && partNumber > 0) return yield* Cause.done();
              if (!body) {
                partNumber++;
                return [{ body: new Uint8Array(), partNumber }] as const;
              }
              return [{ body, partNumber: ++partNumber }] as const;
            }),
          ),
        ).pipe(
          Stream.mapEffect(
            ({ body, partNumber: number }) =>
              operation
                .request(
                  Effect.suspend(() => {
                    operation.progress.bytesSent += body.length;
                    return multipart.uploadPart({
                      upload: handle,
                      partNumber: number,
                      body,
                      contentLength: body.length,
                    });
                  }),
                  { retry: true, retryUnknown: true },
                )
                .pipe(
                  Effect.flatMap((receipt) =>
                    partReceipt(receipt, number, body.length, endpoint.storage.provider),
                  ),
                  Effect.tap(() =>
                    Effect.gen(function* () {
                      operation.progress.bytesAcknowledged += body.length;
                      yield* operation.observe();
                    }),
                  ),
                ),
            { concurrency: plan.concurrency, unordered: true },
          ),
          Stream.runCollect,
        );
        checksum = yield* verify();
        metadata = yield* operation
          .request(
            Effect.suspend(() => {
              unknownCompletion = true;
              operation.mutationStarted = true;
              return multipart.complete({
                upload: handle,
                parts: parts.toSorted((a, b) => a.partNumber - b.partNumber),
                ...(request.condition ? { condition: request.condition } : {}),
              });
            }),
            { retry: true },
          )
          .pipe(
            Effect.catch((error) =>
              Effect.gen(function* () {
                unknownCompletion = error.mutationOutcome === "unknown";
                return yield* reconcile(runtime, operation, request.key, reader.count(), error);
              }),
            ),
          );
        completed = true;
      }

      operation.committed = true;
      operation.progress.phase = "committed";
      return { metadata, checksum, strategy: plan.strategy, progress: { ...operation.progress } };
    }),
  );
  const transfer = reserved ? work : runtime.lease(plan.workspaceBytes, work);
  const bounded = existing ? transfer : runtime.deadline(operation, transfer);
  const receipt = yield* bounded;
  if (!existing) yield* operation.observe("committed");
  return receipt;
});
