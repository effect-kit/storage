import { Cause, Clock, Effect, Exit, Metric, Random, Semaphore } from "effect";
import type { SecureRandom } from "effect/crypto";

import { StorageError, storageError } from "../core/errors.js";
import { makeBudget } from "./budget.js";
import type {
  TransferDiagnostic,
  TransferEndpoint,
  TransferConfiguration,
  TransferProgress,
} from "./models.js";
import type { ResolvedPolicy } from "./planner.js";

const operations = Metric.counter("storage.transfer.operations");
const attempts = Metric.counter("storage.transfer.attempts");
const retries = Metric.counter("storage.transfer.retries");
const cleanupDebt = Metric.counter("storage.transfer.cleanup_debt");
const inFlight = Metric.gauge("storage.transfer.reserved_bytes");

export interface Operation {
  readonly id: string;
  readonly progress: { -readonly [K in keyof TransferProgress]: TransferProgress[K] };
  readonly startedAt: number;
  mutationStarted: boolean;
  committed: boolean;
  readonly request: <A, E = never, R = never>(
    effect: Effect.Effect<A, StorageError | E, R>,
    options?: {
      readonly retry?: boolean;
      readonly retryUnknown?: boolean;
      readonly canRetry?: () => boolean;
    },
  ) => Effect.Effect<A, StorageError | E, R>;
  readonly observe: (phase?: TransferProgress["phase"]) => Effect.Effect<void>;
}

export function makeRuntime(
  endpoint: TransferEndpoint,
  input: TransferConfiguration,
  policy: ResolvedPolicy,
  random: SecureRandom.SecureRandom["Service"],
) {
  const budget = makeBudget(policy.memoryBudgetBytes);
  const requests = Semaphore.makeUnsafe(policy.requestConcurrency);
  let activeRequests = 0;
  let peakActiveRequests = 0;
  let dropped = 0;
  const diagnostics: Array<TransferDiagnostic> = [];

  const diagnostic = (entry: TransferDiagnostic) =>
    Effect.gen(function* () {
      if (diagnostics.length < 128) diagnostics.push(entry);
      else dropped++;
      if (entry.kind === "cleanup" || entry.kind === "unknown-outcome")
        yield* Metric.update(cleanupDebt, 1);
    });

  const begin = Effect.fnUntraced(function* (
    operation: TransferProgress["operation"],
    totalBytes?: number,
  ): Effect.fn.Return<Operation> {
    const id = yield* random.uuidV4();
    const startedAt = yield* Clock.currentTimeMillis;
    const progress = {
      operation,
      phase: "running" as TransferProgress["phase"],
      totalBytes,
      bytesRead: 0,
      bytesSent: 0,
      bytesAcknowledged: 0,
      bytesVerified: 0,
      attempts: 0,
    };
    yield* Metric.update(operations, 1);

    const state: Operation = {
      id,
      progress,
      startedAt,
      mutationStarted: false,
      committed: false,
      request: <A, E = never, R = never>(
        effect: Effect.Effect<A, StorageError | E, R>,
        options: {
          readonly retry?: boolean;
          readonly retryUnknown?: boolean;
          readonly canRetry?: () => boolean;
        } = {},
      ) =>
        Effect.gen(function* () {
          let retry = 0;
          while (true) {
            if (progress.attempts >= policy.maxAttempts)
              return yield* storageError(
                operation,
                endpoint.storage.provider,
                { _tag: "LimitExceeded", limit: policy.maxAttempts },
                state.committed ? "succeeded" : state.mutationStarted ? "unknown" : "not-started",
              );
            const result = yield* requests
              .withPermit(
                Effect.suspend(() => {
                  if (progress.attempts >= policy.maxAttempts)
                    return Effect.fail(
                      storageError(
                        operation,
                        endpoint.storage.provider,
                        { _tag: "LimitExceeded", limit: policy.maxAttempts },
                        state.committed
                          ? "succeeded"
                          : state.mutationStarted
                            ? "unknown"
                            : "not-started",
                      ),
                    );
                  progress.attempts++;
                  activeRequests++;
                  peakActiveRequests = Math.max(peakActiveRequests, activeRequests);
                  return Metric.update(attempts, 1).pipe(
                    Effect.andThen(effect),
                    Effect.ensuring(
                      Effect.sync(() => {
                        activeRequests--;
                      }),
                    ),
                  );
                }),
              )
              .pipe(Effect.result);
            if (result._tag === "Success") return result.success;

            const error = result.failure;
            if (!(error instanceof StorageError)) return yield* Effect.fail(error);
            const reason = error.reason;
            const transient =
              reason._tag === "TransportFailure" ||
              reason._tag === "Throttled" ||
              (reason._tag === "ProviderFailure" &&
                ["InternalError", "ServiceUnavailable", "SlowDown", "RequestTimeout"].includes(
                  reason.code ?? "",
                ));
            if (
              !options.retry ||
              options.canRetry?.() === false ||
              !transient ||
              (error.mutationOutcome === "unknown" && !options.retryUnknown) ||
              progress.attempts >= policy.maxAttempts
            )
              return yield* error;

            const cap = Math.min(
              policy.retryMaxMillis,
              policy.retryBaseMillis * 2 ** Math.min(retry++, 20),
            );
            const jitter = yield* Random.next;
            const requested = reason._tag === "Throttled" ? (reason.retryAfterMillis ?? 0) : 0;
            // Never retry sooner than Retry-After. If it exceeds the operation deadline, fail here.
            const delay = Math.max(requested, Math.floor(cap * (0.5 + jitter * 0.5)));
            const now = yield* Clock.currentTimeMillis;
            if (now + delay >= startedAt + policy.deadlineMillis) return yield* error;
            yield* Metric.update(retries, 1);
            yield* Effect.sleep(delay);
          }
        }),
      observe: (phase = progress.phase) =>
        Effect.gen(function* () {
          progress.phase = phase;
          const observer = input.observer;
          if (!observer) return;
          const result = yield* Effect.exit(
            Effect.suspend(() => observer({ ...progress })).pipe(
              Effect.timeout(policy.observerTimeoutMillis),
            ),
          );
          if (Exit.isFailure(result)) yield* diagnostic({ kind: "observer", operationId: id });
        }),
    };
    return state;
  });

  return {
    endpoint,
    newId: () => random.uuidV4(),
    input,
    policy,
    begin,
    diagnostic,
    diagnostics: Effect.sync(() => ({ entries: [...diagnostics], dropped })),
    resources: Effect.sync(() => ({ ...budget.snapshot(), activeRequests, peakActiveRequests })),
    cleanupRequest: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      requests.withPermit(
        Effect.suspend(() => {
          activeRequests++;
          peakActiveRequests = Math.max(peakActiveRequests, activeRequests);
          return Metric.update(attempts, 1).pipe(
            Effect.andThen(effect),
            Effect.ensuring(
              Effect.sync(() => {
                activeRequests--;
              }),
            ),
          );
        }),
      ),
    lease: <A, E, R>(bytes: number, effect: Effect.Effect<A, E, R>) =>
      budget.withLease(
        bytes,
        Effect.acquireUseRelease(
          Metric.modify(inFlight, bytes),
          () => effect,
          () => Metric.modify(inFlight, -bytes),
        ),
      ),
    deadline: <A, E, R>(operation: Operation, effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit) &&
          Cause.hasInterrupts(exit.cause) &&
          operation.mutationStarted &&
          !operation.committed
            ? diagnostic({ kind: "unknown-outcome", operationId: operation.id })
            : Effect.void,
        ),
        Effect.timeoutOrElse({
          duration: policy.deadlineMillis,
          orElse: () =>
            Effect.fail(
              storageError(
                operation.progress.operation,
                endpoint.storage.provider,
                { _tag: "TransportFailure", code: "TransferDeadlineExceeded" },
                operation.committed
                  ? "succeeded"
                  : operation.mutationStarted
                    ? "unknown"
                    : "not-started",
              ),
            ),
        }),
      ),
  };
}

export type TransferRuntime = ReturnType<typeof makeRuntime>;
