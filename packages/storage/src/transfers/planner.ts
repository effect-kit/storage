import { Effect } from "effect";

import { storageError } from "../errors.js";
import type { WriteCondition } from "../requests.js";
import type { TransferEndpoint, TransferConfiguration } from "./models.js";

const MiB = 1024 * 1024;

export interface ResolvedPolicy {
  readonly memoryBudgetBytes: number;
  readonly partBytes: number;
  readonly partConcurrency: number;
  readonly requestConcurrency: number;
  readonly maxSourceChunkBytes: number;
  readonly maxBytes: number;
  readonly deadlineMillis: number;
  readonly maxAttempts: number;
  readonly retryBaseMillis: number;
  readonly retryMaxMillis: number;
  readonly cleanupTimeoutMillis: number;
  readonly observerTimeoutMillis: number;
}

export function resolvePolicy(input: TransferConfiguration, endpoint: TransferEndpoint) {
  return Effect.gen(function* () {
    const policy: ResolvedPolicy = {
      memoryBudgetBytes: input.memoryBudgetBytes ?? 64 * MiB,
      partBytes: input.partBytes ?? 8 * MiB,
      partConcurrency: input.partConcurrency ?? 3,
      requestConcurrency: input.requestConcurrency ?? 6,
      maxSourceChunkBytes: input.maxSourceChunkBytes ?? MiB,
      maxBytes: input.maxBytes ?? Math.min(endpoint.maximumObjectBytes, 1024 * MiB),
      deadlineMillis: input.deadlineMillis ?? 15 * 60 * 1000,
      maxAttempts: input.maxAttempts ?? 1000,
      retryBaseMillis: input.retryBaseMillis ?? 100,
      retryMaxMillis: input.retryMaxMillis ?? 10000,
      cleanupTimeoutMillis: input.cleanupTimeoutMillis ?? 5000,
      observerTimeoutMillis: input.observerTimeoutMillis ?? 100,
    };
    for (const [field, value] of Object.entries(policy)) {
      if (
        !Number.isSafeInteger(value) ||
        value < (field === "maxBytes" || field === "retryBaseMillis" ? 0 : 1)
      )
        return yield* storageError("transfers.make", endpoint.storage.provider, {
          _tag: "InvalidConfiguration",
          field,
        });
    }
    const limits = [
      endpoint.maximumObjectBytes,
      endpoint.maximumSingleUploadBytes,
      endpoint.transportOverheadBytes,
      input.digest?.workspaceBytes,
    ];
    if (
      limits.some((value) => !Number.isSafeInteger(value) || value < 0) ||
      !input.digest?.algorithm ||
      typeof input.digest.create !== "function"
    )
      return yield* storageError("transfers.make", endpoint.storage.provider, {
        _tag: "InvalidConfiguration",
        field: "endpoint-or-digest",
      });
    if (endpoint.multipart) {
      const { minimumPartBytes, maximumPartBytes, maximumParts } = endpoint.multipart.limits;
      if (
        ![minimumPartBytes, maximumPartBytes, maximumParts].every(
          (value) => Number.isSafeInteger(value) && value > 0,
        ) ||
        minimumPartBytes > maximumPartBytes
      )
        return yield* storageError("transfers.make", endpoint.storage.provider, {
          _tag: "InvalidConfiguration",
          field: "multipart.limits",
        });
    }
    return policy;
  });
}

export interface UploadPlan {
  readonly strategy: "single" | "multipart" | "offset" | "blocks";
  readonly partBytes: number;
  readonly concurrency: number;
  readonly maximumBytes: number;
  readonly workspaceBytes: number;
}

export function planUpload(
  endpoint: TransferEndpoint,
  policy: ResolvedPolicy,
  length: number | undefined,
  inputDigestWorkspace: number,
  strategy: "auto" | "single" | "multipart" | "offset" | "blocks" = "auto",
  serverCopy = false,
  condition?: WriteCondition,
) {
  return Effect.gen(function* () {
    const provider = endpoint.storage.provider;
    const maximumBytes = Math.min(policy.maxBytes, endpoint.maximumObjectBytes);
    if (length !== undefined && (!Number.isSafeInteger(length) || length < 0))
      return yield* storageError(
        "upload.plan",
        provider,
        { _tag: "InvalidInput", field: "source.length" },
        "not-started",
      );
    if (length !== undefined && length > maximumBytes)
      return yield* storageError(
        "upload.plan",
        provider,
        { _tag: "LimitExceeded", limit: maximumBytes },
        "not-started",
      );
    if (!["auto", "single", "multipart", "offset", "blocks"].includes(strategy))
      return yield* storageError(
        "upload.plan",
        provider,
        { _tag: "InvalidInput", field: "strategy" },
        "not-started",
      );

    const native =
      strategy === "offset" || strategy === "blocks"
        ? strategy
        : strategy === "auto" && !endpoint.multipart
          ? endpoint.offsets
            ? "offset"
            : endpoint.blocks
              ? "blocks"
              : undefined
          : undefined;
    if (
      native &&
      (native === "offset" ? !endpoint.offsets || length === undefined : !endpoint.blocks)
    )
      return yield* storageError(
        "upload.plan",
        provider,
        { _tag: "Unsupported", capability: `${native}-known-source` },
        "not-started",
      );
    const single =
      strategy === "single" ||
      (strategy === "auto" &&
        length !== undefined &&
        length <= Math.min(policy.partBytes, endpoint.maximumSingleUploadBytes)) ||
      (!endpoint.multipart && !native);
    let partBytes: number;
    let concurrency: number;
    if (single) {
      partBytes = length ?? maximumBytes;
      concurrency = 1;
      if (strategy === "multipart" || partBytes > endpoint.maximumSingleUploadBytes)
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "Unsupported", capability: "bounded-single-upload" },
          "not-started",
        );
    } else if (native) {
      const alignment = native === "offset" ? (endpoint.offsets?.limits.alignmentBytes ?? 0) : 1;
      const maxPart =
        native === "offset"
          ? (endpoint.offsets?.limits.maximumChunkBytes ?? 0)
          : (endpoint.blocks?.limits.maximumBlockBytes ?? 0);
      const count =
        native === "blocks"
          ? (endpoint.blocks?.limits.maximumBlocks ?? 0)
          : Number.MAX_SAFE_INTEGER;
      if (![alignment, maxPart, count].every((value) => Number.isSafeInteger(value) && value > 0))
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "InvalidConfiguration", field: "native.limits" },
          "not-started",
        );
      partBytes =
        (native === "offset" ? endpoint.offsets?.limits.fixedChunkBytes : undefined) ??
        Math.ceil(
          Math.max(policy.partBytes, Math.ceil((length ?? maximumBytes) / count)) / alignment,
        ) * alignment;
      concurrency = 1;
      if (partBytes > maxPart)
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "LimitExceeded", limit: maxPart },
          "not-started",
        );
      if (
        length !== undefined &&
        Math.max(1, Math.ceil(length / partBytes)) + 3 > policy.maxAttempts
      )
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "LimitExceeded", limit: policy.maxAttempts },
          "not-started",
        );
    } else {
      const multipart = endpoint.multipart;
      if (!multipart)
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "Unsupported", capability: "multipart" },
          "not-started",
        );
      const limits = multipart.limits;
      if (
        condition &&
        limits.completeConditions &&
        !limits.completeConditions.includes(
          condition._tag === "CreateOnly" ? "create-only" : "if-match",
        )
      )
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "Unsupported", capability: "multipart-conditional-completion" },
          "not-started",
        );
      partBytes = Math.max(
        policy.partBytes,
        limits.minimumPartBytes,
        Math.ceil((length ?? maximumBytes) / limits.maximumParts),
      );
      concurrency = policy.partConcurrency;
      if (partBytes > limits.maximumPartBytes)
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "LimitExceeded", limit: limits.maximumPartBytes * limits.maximumParts },
          "not-started",
        );
      if (
        length !== undefined &&
        Math.max(1, Math.ceil(length / partBytes)) + 2 > policy.maxAttempts
      )
        return yield* storageError(
          "upload.plan",
          provider,
          { _tag: "LimitExceeded", limit: policy.maxAttempts },
          "not-started",
        );
    }

    // One producer buffer, retained worker buffers, one upstream chunk, and explicit transport allowance.
    const receiptBytes = single
      ? 0
      : Math.max(1, Math.ceil((length ?? maximumBytes) / partBytes)) * 8192;
    const uploadWorkspaceBytes =
      (serverCopy ? 0 : (concurrency + 1) * Math.max(1, partBytes) + policy.maxSourceChunkBytes) +
      concurrency * endpoint.transportOverheadBytes +
      inputDigestWorkspace +
      receiptBytes;
    const workspaceBytes = Math.max(
      uploadWorkspaceBytes,
      native
        ? policy.maxSourceChunkBytes * 2 + endpoint.transportOverheadBytes + inputDigestWorkspace
        : 0,
    );
    if (!Number.isSafeInteger(workspaceBytes) || workspaceBytes > policy.memoryBudgetBytes)
      return yield* storageError(
        "upload.plan",
        provider,
        { _tag: "QuotaExceeded", limit: policy.memoryBudgetBytes },
        "not-started",
      );
    return {
      strategy: single ? ("single" as const) : (native ?? ("multipart" as const)),
      partBytes,
      concurrency,
      maximumBytes,
      workspaceBytes,
    };
  });
}
