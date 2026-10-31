/* oxlint-disable no-await-in-loop -- Streamed response bytes are consumed in order under a hard size limit. */
import { Context, Effect, Layer, Option, Redacted, Result, Stream } from "effect";

import { Storage, StorageSigning } from "@effect-kit/storage";
import type { ObjectRef, StorageError, PutRequest } from "@effect-kit/storage";
export interface SuiteReport {
  checks: string[];
  cleanupPending: number;
  failure?: { operation: string; reason: string };
}
export type ProviderLayer = Layer.Layer<Storage | TransferDriver, StorageError>;
import { Transfers, TransferDriver, UploadSource } from "@effect-kit/storage/transfers";

function ensure(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

/** Bounded, exact-key ownership: never enumerate a bucket for cleanup or change native policy. */
export const portableSuite = Effect.fnUntraced(function* (
  provider: ProviderLayer,
  prefix: string,
  report: SuiteReport,
  suite = "portable",
) {
  const context = yield* Layer.build(provider);
  const storage = Context.get(context, Storage);
  const payload = new Uint8Array(32768).map((_, index) => index % 251);
  const owned: Array<{ ref?: ObjectRef }> = [];
  const checks = report.checks;
  const cleanup = Effect.gen(function* () {
    for (const entry of owned.toReversed()) {
      const ref = entry.ref;
      if (!ref) {
        report.cleanupPending++;
        continue;
      }
      const result = yield* Effect.result(storage.delete({ ref }));
      if (Result.isFailure(result) && result.failure.reason._tag !== "NotFound") {
        report.cleanupPending++;
        continue;
      }
      const remaining = yield* Effect.result(storage.head({ ref }));
      if (Result.isSuccess(remaining) || remaining.failure.reason._tag !== "NotFound")
        report.cleanupPending++;
    }
  });
  yield* Effect.addFinalizer(() => cleanup);
  const put = (key: string, body: PutRequest["body"], extra: Partial<PutRequest> = {}) => {
    const target = prefix + key;
    // Record the exact key before I/O; a failed response is not proof the write did not happen.
    const entry: { ref?: ObjectRef } = {};
    owned.push(entry);
    return storage.put({ key: target, body, ...extra }).pipe(
      Effect.tap((metadata) =>
        Effect.sync(() => {
          entry.ref = metadata.ref;
        }),
      ),
    );
  };
  const metadata = yield* put("object", payload, {
    contentType: "application/octet-stream",
    ...(storage.capabilities.putConditions.includes("create-only")
      ? { condition: { _tag: "CreateOnly" } }
      : {}),
  });
  checks.push("put");
  const ref = metadata.ref;
  const head = yield* storage.head({ ref });
  ensure(head.size === payload.length, "head size mismatch");
  checks.push("head");
  const bytes = yield* Storage.getBytes({ ref, maxBytes: payload.length }).pipe(
    Effect.provideService(Storage, storage),
  );
  ensure(
    bytes.length === payload.length && bytes.every((byte, index) => byte === payload[index]),
    "download integrity mismatch",
  );
  checks.push("verified-read");
  if (storage.capabilities.putConditions.includes("create-only")) {
    const duplicate = yield* Effect.result(
      storage.put({ key: `${prefix}object`, body: payload, condition: { _tag: "CreateOnly" } }),
    );
    ensure(
      Result.isFailure(duplicate) && duplicate.failure.reason._tag === "PreconditionFailed",
      "create-only did not reject",
    );
    checks.push("create-only");
  }
  if (storage.capabilities.ranges.includes("suffix")) {
    const suffix = yield* Storage.getBytes({
      ref,
      range: { _tag: "Suffix", length: 7 },
      maxBytes: payload.length,
    }).pipe(Effect.provideService(Storage, storage));
    ensure(
      suffix.length === 7 &&
        suffix.every((byte, index) => byte === payload[payload.length - 7 + index]),
      "range mismatch",
    );
    checks.push("range");
  }
  const page = yield* storage.listPage({ prefix, limit: 100 });
  ensure(
    page.entries.every(
      (entry) => entry._tag !== "Object" || entry.metadata.ref.key.startsWith(prefix),
    ),
    "listing escaped prefix",
  );
  ensure(
    page.entries.some((entry) => entry._tag === "Object" && entry.metadata.ref.key === ref.key),
    "listing omitted fixture",
  );
  checks.push("scoped-list");
  yield* put("stream", Stream.make(payload.subarray(0, 16384), payload.subarray(16384)), {
    contentLength: payload.length,
  });
  checks.push("streamed-put");
  const endpoint = Context.get(context, TransferDriver);
  const transfers = yield* Transfers.make({ maxBytes: 1024 * 1024, maxAttempts: 8 }).pipe(
    Effect.provideService(Storage, storage),
    Effect.provideService(TransferDriver, endpoint),
  );
  const key = `${prefix}managed`;
  const managed: { ref?: ObjectRef } = {};
  owned.push(managed);
  const receipt = yield* transfers.upload({ key, source: UploadSource.bytes(payload) });
  managed.ref = receipt.metadata.ref;
  ensure(receipt.checksum.algorithm === "sha256", "managed upload digest mismatch");
  let downloaded = 0;
  const verified = yield* transfers.download({
    ref: receipt.metadata.ref,
    expectedChecksum: receipt.checksum,
    target: {
      write: (chunk) =>
        Effect.sync(() => {
          ensure(
            chunk.every((byte, index) => byte === payload[downloaded + index]),
            "managed read mismatch",
          );
          downloaded += chunk.length;
        }),
    },
  });
  ensure(
    verified.progress.phase === "verified" && downloaded === payload.length,
    "managed download unverified",
  );
  checks.push("managed-upload-sha256");
  if (suite === "signing") {
    const service = Context.getOption(context, StorageSigning);
    if (Option.isNone(service)) return yield* Effect.fail(new Error("Signing unavailable"));
    const signing = service.value;
    const request = yield* signing.sign({ ref, method: "GET", expiresInSeconds: 60 });
    const result = yield* Effect.tryPromise({
      try: async () => {
        const response = await fetch(Redacted.value(request.url), {
          headers: Redacted.value(request.headers),
          redirect: "error",
          signal: AbortSignal.timeout(15000),
        });
        ensure(response.ok && response.body, "signed read failed");
        const reader = response.body.getReader();
        let size = 0;
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          ensure(
            value.every((byte, index) => byte === payload[size + index]),
            "signed read content mismatch",
          );
          size += value.length;
          if (size > payload.length) {
            await reader.cancel();
            throw new Error("signed read oversized");
          }
        }
        ensure(size === payload.length, "signed read size mismatch");
      },
      catch: () => new Error("Signed read failed"),
    });
    void result;
    checks.push("signed-read");
  }
  report.checks = checks;
});
