import { Effect, Stream } from "effect";

import type { StorageError } from "../core/errors.js";
import { storageError } from "../core/errors.js";
import type { ObjectMetadata } from "../core/object.js";
import { Checksum } from "../core/object.js";
import type { GetRequest } from "../core/requests.js";
import { HeadRequest } from "../core/requests.js";
import { decodeRequest } from "../internal/request-validation.js";
import type { DownloadRequest, TransferEndpoint, TransferReceipt } from "./models.js";
import { sameChecksum } from "./reader.js";
import type { Operation, TransferRuntime } from "./runtime.js";

export function pinnedRequest(
  endpoint: TransferEndpoint,
  metadata: ObjectMetadata,
): GetRequest | undefined {
  const ref = metadata.ref;
  if (ref.version && endpoint.storage.capabilities.versionedReads !== "none") return { ref };
  if (
    metadata.etag &&
    metadata.etag !== "*" &&
    !metadata.etag.startsWith("W/") &&
    endpoint.storage.capabilities.getConditions.includes("if-match")
  )
    return {
      ref: { storeId: ref.storeId, key: ref.key },
      condition: { _tag: "IfMatch", etag: metadata.etag },
    };
  return undefined;
}

export const download = Effect.fn("Transfers.download")(function* <E, R>(
  runtime: TransferRuntime,
  request: DownloadRequest<E, R>,
  existing?: Operation,
  selected?: ObjectMetadata,
  reserved = false,
): Effect.fn.Return<TransferReceipt, StorageError | E, R> {
  const operation = existing ?? (yield* runtime.begin("download"));
  const { endpoint, policy } = runtime;
  const provider = endpoint.storage.provider;
  const workspace =
    policy.maxSourceChunkBytes * 2 +
    endpoint.transportOverheadBytes +
    runtime.input.digest.workspaceBytes;
  if (workspace > policy.memoryBudgetBytes)
    return yield* storageError("download", provider, {
      _tag: "QuotaExceeded",
      limit: policy.memoryBudgetBytes,
    });
  if (
    request.expectedChecksum &&
    (request.expectedChecksum.scope !== "full-object" ||
      request.expectedChecksum.algorithm !== runtime.input.digest.algorithm)
  )
    return yield* storageError("download", provider, {
      _tag: "Unsupported",
      capability: "expected-full-object-checksum",
    });
  if (!request.target || typeof request.target.write !== "function")
    return yield* storageError("download", provider, { _tag: "InvalidInput", field: "target" });
  if (request.expectedChecksum)
    yield* decodeRequest(Checksum, request.expectedChecksum, "download", provider);

  const target = yield* decodeRequest(
    HeadRequest,
    {
      ...(request.key !== undefined ? { key: request.key } : {}),
      ...(request.ref !== undefined ? { ref: request.ref } : {}),
    },
    "download",
    provider,
  );
  const work = Effect.gen(function* () {
    const metadata =
      selected ?? (yield* operation.request(endpoint.storage.head(target), { retry: true }));
    const pinned = pinnedRequest(endpoint, metadata);
    const maximum = Math.min(policy.maxBytes, endpoint.maximumObjectBytes);
    if (metadata.size !== undefined && metadata.size > maximum)
      return yield* storageError("download", provider, { _tag: "LimitExceeded", limit: maximum });
    operation.progress.totalBytes = metadata.size;
    const hash = runtime.input.digest.create();
    let offset = 0;
    let targetFailed = false;
    let complete = false;

    yield* operation.request(
      Effect.suspend(() =>
        Effect.scoped(
          Effect.gen(function* () {
            if (
              offset > 0 &&
              (!pinned ||
                !endpoint.storage.capabilities.ranges.includes("from") ||
                metadata.size === undefined)
            )
              return yield* storageError("download", provider, {
                _tag: "Unsupported",
                capability: "pinned-download-continuation",
              });
            const start = offset;
            const response = yield* endpoint.storage.get({
              ...(pinned ?? target),
              ...(offset > 0 ? { range: { _tag: "From" as const, start: offset } } : {}),
            });
            if (response._tag !== "Object")
              return yield* storageError("download", provider, { _tag: "NotModified" });
            if (pinned && metadata.etag !== undefined && response.metadata.etag !== metadata.etag)
              return yield* storageError("download", provider, { _tag: "PreconditionFailed" });
            if (
              offset > 0 &&
              (!response.range ||
                response.range.start !== offset ||
                response.range.totalSize !== metadata.size)
            )
              return yield* storageError("download", provider, { _tag: "IntegrityMismatch" });

            yield* Stream.runForEach(response.body, (chunk) =>
              Effect.gen(function* () {
                if (chunk.buffer.byteLength > policy.maxSourceChunkBytes)
                  return yield* storageError("download", provider, {
                    _tag: "LimitExceeded",
                    limit: policy.maxSourceChunkBytes,
                  });
                if (offset + chunk.length > maximum)
                  return yield* storageError("download", provider, {
                    _tag: "LimitExceeded",
                    limit: maximum,
                  });
                if (metadata.size !== undefined && offset + chunk.length > metadata.size)
                  return yield* storageError("download", provider, {
                    _tag: "LengthMismatch",
                    expected: metadata.size,
                    actual: offset + chunk.length,
                  });
                operation.progress.bytesRead += chunk.length;
                hash.update(chunk);
                targetFailed = true;
                yield* request.target.write(chunk);
                targetFailed = false;
                offset += chunk.length;
                operation.progress.bytesAcknowledged = offset;
                yield* operation.observe();
              }),
            );
            const received = offset - start;
            if (response.contentLength !== undefined && received !== response.contentLength)
              return yield* storageError("download", provider, {
                _tag: "LengthMismatch",
                expected: response.contentLength,
                actual: received,
              });
            if (metadata.size !== undefined && offset !== metadata.size)
              return yield* storageError("download", provider, {
                _tag: "LengthMismatch",
                expected: metadata.size,
                actual: offset,
              });
            complete = true;
          }),
        ),
      ),
      {
        retry: true,
        canRetry: () =>
          !targetFailed &&
          (offset === 0 || (pinned !== undefined && offset < (metadata.size ?? 0))),
      },
    );

    const checksum = hash.finish();
    const expected =
      request.expectedChecksum ??
      metadata.checksums?.find(
        (candidate) =>
          candidate.scope === "full-object" && candidate.algorithm === checksum.algorithm,
      );
    if (!complete || (expected && !sameChecksum(checksum, expected)))
      return yield* storageError("download", provider, { _tag: "IntegrityMismatch" });
    operation.progress.bytesVerified = offset;
    operation.progress.phase = "verified";
    return {
      metadata,
      checksum,
      strategy: "download" as const,
      progress: { ...operation.progress },
    };
  });
  const transfer = reserved ? work : runtime.lease(workspace, work);
  const bounded = existing ? transfer : runtime.deadline(operation, transfer);
  const receipt = yield* bounded;
  if (!existing) yield* operation.observe("verified");
  return receipt;
});
