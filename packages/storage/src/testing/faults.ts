import { Cause, Effect, Exit, Predicate, Ref, Schema, Stream } from "effect";

import { StorageReason, storageError } from "../core/errors.js";
import type { StorageError } from "../core/errors.js";
import type { GetRequest, PutRequest } from "../core/requests.js";
import { Storage } from "../core/storage.js";
import type { StorageService } from "../core/storage.js";
import { failBody } from "./body-failure.js";

const Operation = Schema.Literals(["put", "get", "head", "delete", "listPage"]);
const FaultStep = Schema.Struct({
  operation: Operation,
  delayMillis: Schema.optional(Schema.Natural),
  beforeFailure: Schema.optional(StorageReason),
  afterFailure: Schema.optional(StorageReason),
  bodyFailure: Schema.optional(
    Schema.Struct({
      afterBytes: Schema.Natural,
      reason: StorageReason,
    }),
  ),
  cleanupFailure: Schema.optional(Schema.NonEmptyString),
  observerFailure: Schema.optional(Schema.NonEmptyString),
});

export type FaultStep = typeof FaultStep.Type;

export interface FaultAttempt {
  readonly index: number;
  readonly operation: typeof Operation.Type;
  readonly status:
    | "started"
    | "acquired"
    | "closed"
    | "succeeded"
    | "failed"
    | "interrupted"
    | "defect";
}

export interface FaultDiagnostic {
  readonly index: number;
  readonly operation: typeof Operation.Type;
  readonly kind: "cleanup" | "observer";
  readonly code: string;
}

export interface FaultController {
  readonly storage: StorageService;
  readonly attempts: Effect.Effect<ReadonlyArray<FaultAttempt>>;
  readonly diagnostics: Effect.Effect<ReadonlyArray<FaultDiagnostic>>;
  readonly assertDone: Effect.Effect<void>;
}

export const make = Effect.fnUntraced(function* (
  base: StorageService,
  script: ReadonlyArray<FaultStep>,
): Effect.fn.Return<FaultController, StorageError> {
  const steps = yield* Schema.decodeUnknownEffect(Schema.Array(FaultStep), {
    onExcessProperty: "error",
  })(script).pipe(
    Effect.mapError(() =>
      storageError("faults", base.provider, { _tag: "InvalidConfiguration", field: "script" }),
    ),
  );

  if (
    steps.some(
      (step) =>
        (step.cleanupFailure && step.operation !== "get") ||
        (step.bodyFailure && step.operation !== "put" && step.operation !== "get"),
    )
  ) {
    return yield* storageError("faults", base.provider, {
      _tag: "InvalidConfiguration",
      field: "script.phase",
    });
  }

  const index = yield* Ref.make(0);
  const attempts = yield* Ref.make<ReadonlyArray<FaultAttempt>>([]);
  const diagnostics = yield* Ref.make<ReadonlyArray<FaultDiagnostic>>([]);
  const recordDiagnostic = (
    step: FaultStep,
    position: number,
    kind: "cleanup" | "observer",
    code: string,
  ) =>
    Ref.update(diagnostics, (current) => [
      ...current,
      { operation: step.operation, index: position, kind, code },
    ]);

  const perform = <A, E, R>(
    operation: typeof Operation.Type,
    execute: (step: FaultStep, position: number) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | StorageError, R> =>
    Effect.gen(function* () {
      const position = yield* Ref.getAndUpdate(index, (current) => current + 1);
      const step = steps[position];
      if (step === undefined || step.operation !== operation) {
        return yield* Effect.die(
          new Error(
            step === undefined
              ? "Storage fault script exhausted"
              : "Storage fault script operation mismatch",
          ),
        );
      }

      yield* Ref.update(attempts, (current) => [
        ...current,
        {
          index: position,
          operation,
          status: "started",
        } satisfies FaultAttempt,
      ]);

      const effect = Effect.gen(function* () {
        if (step.delayMillis) yield* Effect.sleep(step.delayMillis);
        if (step.beforeFailure) {
          return yield* storageError(
            operation,
            base.provider,
            step.beforeFailure,
            operation === "put" || operation === "delete" ? "not-started" : "not-applicable",
          );
        }

        const value = yield* execute(step, position);
        if (step.observerFailure)
          yield* recordDiagnostic(step, position, "observer", step.observerFailure);
        if (step.afterFailure) {
          return yield* storageError(
            operation,
            base.provider,
            step.afterFailure,
            operation === "put" || operation === "delete" ? "unknown" : "not-applicable",
          );
        }
        return value;
      });

      return yield* Effect.onExit(effect, (exit) =>
        Ref.update(attempts, (current) =>
          current.map((attempt) =>
            attempt.index !== position
              ? attempt
              : {
                  ...attempt,
                  status: Exit.isSuccess(exit)
                    ? operation === "get"
                      ? "acquired"
                      : "succeeded"
                    : Cause.hasInterrupts(exit.cause)
                      ? "interrupted"
                      : Cause.hasDies(exit.cause)
                        ? "defect"
                        : "failed",
                },
          ),
        ),
      );
    });

  const storage = Storage.make({
    ...base,
    put: <E = never, R = never>(request: PutRequest<E, R>) =>
      perform("put", (step) => {
        if (!step.bodyFailure) return base.put(request);

        const source = Predicate.isUint8Array(request.body)
          ? Stream.succeed(request.body)
          : request.body;
        return base.put({
          ...request,
          body: failBody(
            source,
            step.bodyFailure.afterBytes,
            storageError("put", base.provider, step.bodyFailure.reason, "unknown"),
          ),
        });
      }),
    get: (request: GetRequest) =>
      perform("get", (step, position) =>
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.gen(function* () {
              yield* Ref.update(attempts, (current) =>
                current.map((attempt) =>
                  attempt.index === position && attempt.status === "acquired"
                    ? { ...attempt, status: "closed" as const }
                    : attempt,
                ),
              );
              if (step.cleanupFailure) {
                yield* recordDiagnostic(step, position, "cleanup", step.cleanupFailure);
              }
            }),
          );

          const response = yield* base.get(request);
          if (response._tag === "NotModified") return response;

          const body =
            step.bodyFailure === undefined
              ? response.body
              : failBody(
                  response.body,
                  step.bodyFailure.afterBytes,
                  storageError("get", base.provider, step.bodyFailure.reason),
                );

          return {
            ...response,
            body: Stream.onExit(body, (exit) =>
              Ref.update(attempts, (current) =>
                current.map((attempt) =>
                  attempt.index !== position
                    ? attempt
                    : {
                        ...attempt,
                        status: Exit.isSuccess(exit)
                          ? "succeeded"
                          : Cause.hasInterrupts(exit.cause)
                            ? "interrupted"
                            : Cause.hasDies(exit.cause)
                              ? "defect"
                              : "failed",
                      },
                ),
              ),
            ),
          };
        }),
      ),
    head: (request) => perform("head", () => base.head(request)),
    delete: (request) => perform("delete", () => base.delete(request)),
    listPage: (request) => perform("listPage", () => base.listPage(request)),
  });

  return {
    storage,
    attempts: Ref.get(attempts).pipe(
      Effect.map((current) => current.map((attempt) => ({ ...attempt }))),
    ),
    diagnostics: Ref.get(diagnostics).pipe(
      Effect.map((current) => current.map((diagnostic) => ({ ...diagnostic }))),
    ),
    assertDone: Effect.gen(function* () {
      const consumed = yield* Ref.get(index);
      if (consumed !== steps.length)
        return yield* Effect.die(new Error("Storage fault script has unconsumed steps"));
    }),
  };
});
