import { Clock, Context, Effect, Layer, Schema } from "effect";

import type { StorageError } from "./errors.js";
import type { Checksum, ObjectHeaders, ObjectMetadata, ObjectRef } from "./object.js";
import { Checksum as ChecksumSchema } from "./object.js";
import type { WriteCondition } from "./requests.js";
import { SessionId } from "./sessions/checkpoint.js";
import type { SessionCheckpoint, SessionStatus, UploadCheckpoint } from "./sessions/checkpoint.js";
import { runDownload } from "./sessions/download.js";
import type { RestartableDownloadTarget } from "./sessions/download.js";
import { UploadSessionStore, sessionError } from "./sessions/store.js";
import { abortUpload, fingerprint, reconcileUpload, runUpload } from "./sessions/upload.js";
import type { SessionOptions } from "./sessions/upload.js";
import { pinnedRequest } from "./transfers/download.js";
import { ManagedTransferRuntime } from "./transfers/managed-runtime.js";
import { planUpload } from "./transfers/planner.js";
import type { UploadSource } from "./transfers/sources.js";
import { validateUpload } from "./transfers/upload.js";

export { UploadSessionStore } from "./sessions/store.js";
export type {
  CheckpointProtection,
  SessionOwnership,
  SessionStoreOptions,
} from "./sessions/store.js";
export {
  SessionId,
  SessionCheckpoint,
  UploadCheckpoint,
  DownloadCheckpoint,
  BatchCheckpoint,
} from "./sessions/checkpoint.js";
export type { SessionStatus } from "./sessions/checkpoint.js";
export type { RestartableDownloadTarget } from "./sessions/download.js";
export type { SessionOptions } from "./sessions/upload.js";

export interface CreateUpload<E = never, R = never> {
  readonly id?: string;
  readonly key: string;
  readonly source: UploadSource<E, R>;
  readonly headers?: ObjectHeaders;
  readonly condition?: WriteCondition;
}
export interface SessionsService {
  readonly create: <E, R>(
    request: CreateUpload<E, R>,
  ) => Effect.Effect<string, StorageError | E, R>;
  readonly resume: <E, R>(
    id: string,
    source: UploadSource<E, R>,
  ) => Effect.Effect<ObjectMetadata, StorageError | E, R>;
  readonly complete: <E, R>(
    id: string,
    source: UploadSource<E, R>,
  ) => Effect.Effect<ObjectMetadata, StorageError | E, R>;
  readonly status: (id: string) => Effect.Effect<SessionStatus, StorageError>;
  readonly reconcile: (id: string) => Effect.Effect<SessionStatus, StorageError>;
  readonly abort: (id: string) => Effect.Effect<void, StorageError>;
  readonly forget: (id: string) => Effect.Effect<void, StorageError>;
  readonly deliverEvents: <E, R>(
    id: string,
    deliver: (event: {
      readonly id: string;
      readonly kind: "completed" | "aborted";
      readonly sessionId: string;
      readonly metadata?: ObjectMetadata | undefined;
    }) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, StorageError | E, R>;
  readonly createDownload: (request: {
    readonly id: string;
    readonly ref: ObjectRef;
    readonly expectedChecksum?: Checksum;
  }) => Effect.Effect<string, StorageError>;
  readonly resumeDownload: <E, R>(
    id: string,
    target: RestartableDownloadTarget<E, R>,
  ) => Effect.Effect<ObjectMetadata, StorageError | E, R>;
  readonly abortDownload: <E, R>(
    id: string,
    target: RestartableDownloadTarget<E, R>,
  ) => Effect.Effect<void, StorageError | E, R>;
  readonly createBatch: (sessionIds: ReadonlyArray<string>) => Effect.Effect<string, StorageError>;
  readonly processBatch: <E, R>(
    id: string,
    run: (sessionId: string) => Effect.Effect<unknown, E, R>,
  ) => Effect.Effect<
    ReadonlyArray<{ readonly sessionId: string; readonly state: string }>,
    StorageError,
    R
  >;
}

export class UploadSessions extends Context.Service<UploadSessions, SessionsService>()(
  "@effect-kit/storage/UploadSessions",
) {
  static readonly make = Effect.fnUntraced(function* (options: SessionOptions) {
    const store = yield* UploadSessionStore;
    const runtime = yield* ManagedTransferRuntime;
    const { endpoint, input: policy, policy: resolved } = runtime;
    const ttl = options.expiresInMillis ?? 24 * 60 * 60 * 1000;
    if (!options.configurationId || !Number.isSafeInteger(ttl) || ttl < 1)
      return yield* sessionError("InvalidSessionPolicy");
    const environment = { runtime, store, options };
    const load = Effect.fnUntraced(function* (id: string, allowExpired = false) {
      const checkpoint = yield* store.read(id);
      if (!checkpoint) return yield* sessionError("SessionNotFound");
      if (
        checkpoint.provider !== endpoint.storage.provider ||
        checkpoint.configurationId !== options.configurationId ||
        checkpoint.storeId !== endpoint.storage.storeId
      )
        return yield* sessionError("ChangedConfiguration");
      const settling =
        checkpoint.kind !== "batch" &&
        ["verifying", "verified", "completed"].includes(checkpoint.state);
      if (!allowExpired && !settling && checkpoint.expiresAt <= (yield* Clock.currentTimeMillis))
        return yield* sessionError("SessionExpired");
      return checkpoint;
    });
    const base = Effect.fnUntraced(function* (id: string) {
      const now = yield* Clock.currentTimeMillis;
      return {
        version: 1 as const,
        id,
        namespace: store.namespace,
        revision: 0,
        configurationId: options.configurationId,
        provider: endpoint.storage.provider,
        storeId: endpoint.storage.storeId,
        createdAt: now,
        updatedAt: now,
        expiresAt: now + ttl,
      };
    });
    const status = Effect.fnUntraced(function* (
      id: string,
    ): Effect.fn.Return<SessionStatus, StorageError> {
      const checkpoint = yield* load(id, true);
      return {
        id,
        kind: checkpoint.kind,
        state:
          checkpoint.kind === "batch"
            ? checkpoint.entries.every((entry) => entry.state === "completed")
              ? "completed"
              : "pending"
            : checkpoint.state,
        revision: checkpoint.revision,
        expired: checkpoint.expiresAt <= (yield* Clock.currentTimeMillis),
        ...(checkpoint.kind === "upload"
          ? { completedParts: checkpoint.parts.length, metadata: checkpoint.metadata }
          : checkpoint.kind === "download"
            ? { offset: checkpoint.offset, metadata: checkpoint.metadata }
            : {}),
      };
    });
    const resume = <E, R>(id: string, source: UploadSource<E, R>) =>
      store.exclusive(
        Effect.gen(function* () {
          const checkpoint = yield* load(id);
          if (checkpoint.kind !== "upload") return yield* sessionError("WrongSessionKind");
          return yield* runUpload(environment, checkpoint, source);
        }),
      );
    return UploadSessions.of({
      create: (request) =>
        store.exclusive(
          Effect.gen(function* () {
            if (request.id !== undefined)
              yield* Schema.decodeUnknownEffect(SessionId)(request.id).pipe(
                Effect.mapError(() => sessionError("InvalidSessionId")),
              );
            if (!endpoint.multipart || !endpoint.handles)
              return yield* sessionError("DurableUploadUnsupported");
            const headers = yield* validateUpload(runtime, request);
            const { key, source, id: suppliedId } = request;
            const condition = request.condition ? { ...request.condition } : undefined;
            const plan = yield* planUpload(
              endpoint,
              resolved,
              source.length,
              policy.digest.workspaceBytes * 2 + (source.workspaceBytes ?? 0),
              "multipart",
            );
            if (
              Math.max(1, Math.ceil((source.length ?? 0) / plan.partBytes)) + 5 >
              resolved.maxAttempts
            )
              return yield* sessionError("SessionAttemptLimit");
            const preparation = yield* runtime.begin("upload", source.length);
            const checksum = yield* runtime.deadline(
              preparation,
              runtime.lease(plan.workspaceBytes, fingerprint(environment, source, plan.partBytes)),
            );
            const id = suppliedId ?? (yield* runtime.newId());
            const checkpoint: UploadCheckpoint = {
              ...(yield* base(id)),
              kind: "upload",
              operationId: yield* runtime.newId(),
              state: "created",
              key,
              headers,
              ...(condition ? { condition } : {}),
              length: source.length ?? 0,
              checksum,
              partBytes: plan.partBytes,
              parts: [],
              attempts: 0,
              events: [],
            };
            yield* store.write(checkpoint, undefined);
            return id;
          }),
        ),
      resume,
      complete: resume,
      // Read-only polling must not wait behind a transfer that owns the mutation scope.
      status,
      reconcile: (id) =>
        store.exclusive(
          Effect.gen(function* () {
            const checkpoint = yield* load(id, true);
            if (checkpoint.kind !== "upload") return yield* sessionError("WrongSessionKind");
            yield* reconcileUpload(environment, checkpoint);
            return yield* status(id);
          }),
        ),
      abort: (id) =>
        store.exclusive(
          Effect.gen(function* () {
            const checkpoint = yield* load(id, true);
            if (checkpoint.kind !== "upload") return yield* sessionError("WrongSessionKind");
            yield* abortUpload(environment, checkpoint);
          }),
        ),
      deliverEvents: (id, deliver) =>
        store.exclusive(
          Effect.gen(function* () {
            let checkpoint = yield* load(id, true);
            if (checkpoint.kind !== "upload") return yield* sessionError("WrongSessionKind");
            for (const event of checkpoint.events) {
              if (event.delivered) continue;
              yield* deliver({
                id: event.id,
                kind: event.kind,
                sessionId: id,
                metadata: checkpoint.metadata,
              }).pipe(
                Effect.timeoutOrElse({
                  duration: resolved.deadlineMillis,
                  orElse: () => Effect.fail(sessionError("EventDeliveryTimeout", true)),
                }),
              );
              const saved: SessionCheckpoint = yield* store.write(
                {
                  ...checkpoint,
                  events: checkpoint.events.map((candidate) =>
                    candidate.id === event.id ? { ...candidate, delivered: true } : candidate,
                  ),
                },
                checkpoint.revision,
              );
              if (saved.kind !== "upload") return yield* sessionError("WrongSessionKind");
              checkpoint = saved;
            }
          }),
        ),
      createDownload: (request) =>
        store.exclusive(
          Effect.gen(function* () {
            yield* Schema.decodeUnknownEffect(SessionId)(request.id).pipe(
              Effect.mapError(() => sessionError("InvalidSessionId")),
            );
            if (!policy.digest.create().snapshot)
              return yield* sessionError("DigestSnapshotRequired");
            const lookup = yield* runtime.begin("download");
            const metadata = yield* runtime.deadline(
              lookup,
              lookup.request(endpoint.storage.head({ ref: request.ref })),
            );
            if (
              !pinnedRequest(endpoint, metadata) ||
              metadata.size === undefined ||
              metadata.size > resolved.maxBytes
            )
              return yield* sessionError("PinnedSizedDownloadRequired");
            if (request.expectedChecksum)
              yield* Schema.decodeUnknownEffect(ChecksumSchema)(request.expectedChecksum).pipe(
                Effect.mapError(() => sessionError("InvalidChecksum")),
              );
            if (
              request.expectedChecksum &&
              (request.expectedChecksum.scope !== "full-object" ||
                request.expectedChecksum.algorithm !== policy.digest.algorithm)
            )
              return yield* sessionError("UnsupportedChecksum");
            yield* store.write(
              {
                ...(yield* base(request.id)),
                kind: "download",
                state: "downloading",
                metadata,
                length: metadata.size,
                targetId: request.id,
                offset: 0,
                ...(request.expectedChecksum ? { expectedChecksum: request.expectedChecksum } : {}),
              },
              undefined,
            );
            return request.id;
          }),
        ),
      abortDownload: (id, target) =>
        store.exclusive(
          Effect.gen(function* () {
            const checkpoint = yield* load(id, true);
            if (
              checkpoint.kind !== "download" ||
              target.id !== checkpoint.targetId ||
              (checkpoint.targetIdentity &&
                checkpoint.targetIdentity !== (target.identity ?? target.id))
            )
              return yield* sessionError("ForeignDownloadTarget");
            if (checkpoint.state === "aborted") return;
            if (["completed", "verified"].includes(checkpoint.state))
              return yield* sessionError("ReconciliationRequired", true);
            const discard = target.discard?.bind(target);
            if (!discard) return yield* sessionError("TargetCleanupUnsupported");
            const saved = yield* store.write(
              { ...checkpoint, state: "aborting" },
              checkpoint.revision,
            );
            yield* discard().pipe(
              Effect.timeoutOrElse({
                duration: resolved.cleanupTimeoutMillis,
                orElse: () => Effect.fail(sessionError("CleanupPending", true)),
              }),
            );
            if (saved.kind !== "download") return yield* sessionError("WrongSessionKind");
            yield* store.write({ ...saved, state: "aborted" }, saved.revision);
          }),
        ),
      forget: (id) =>
        store.exclusive(
          Effect.gen(function* () {
            const checkpoint = yield* load(id, true);
            const terminal =
              checkpoint.kind === "batch"
                ? checkpoint.entries.every((entry) => entry.state === "completed")
                : ["completed", "aborted"].includes(checkpoint.state);
            if (
              !terminal ||
              (checkpoint.kind === "upload" && checkpoint.events.some((event) => !event.delivered))
            )
              return yield* sessionError("SessionRetentionRequired");
            if (checkpoint.kind === "batch") {
              for (const entry of checkpoint.entries) {
                const child = yield* store.read(entry.sessionId);
                if (child?.kind === "upload" && child.events.some((event) => !event.delivered))
                  return yield* sessionError("SessionRetentionRequired");
              }
            }
            yield* store.remove(id, checkpoint.revision);
          }),
        ),
      resumeDownload: (id, target) =>
        store.exclusive(
          Effect.gen(function* () {
            const checkpoint = yield* load(id);
            if (checkpoint.kind !== "download") return yield* sessionError("WrongSessionKind");
            return yield* runDownload(environment, checkpoint, target);
          }),
        ),
      createBatch: (ids) =>
        store.exclusive(
          Effect.gen(function* () {
            if (ids.length > 10000 || new Set(ids).size !== ids.length)
              return yield* sessionError("InvalidBatch");
            for (const id of ids) {
              const session = yield* load(id);
              if (session.kind === "batch") return yield* sessionError("NestedBatch");
            }
            const id = yield* runtime.newId();
            yield* store.write(
              {
                ...(yield* base(id)),
                kind: "batch",
                entries: ids.map((sessionId) => ({ sessionId, state: "pending" as const })),
              },
              undefined,
            );
            return id;
          }),
        ),
      processBatch: Effect.fnUntraced(function* <E, R>(
        id: string,
        run: (id: string) => Effect.Effect<unknown, E, R>,
      ) {
        const checkpoint = yield* store.exclusive(load(id));
        if (checkpoint.kind !== "batch") return yield* sessionError("WrongSessionKind");
        for (const entry of checkpoint.entries) {
          const current = yield* store.exclusive(load(entry.sessionId));
          if (current.kind !== "batch" && current.state !== "completed")
            yield* run(entry.sessionId).pipe(Effect.result);
          yield* store.exclusive(
            Effect.gen(function* () {
              const batch = yield* load(id);
              const observed = yield* load(entry.sessionId);
              if (batch.kind !== "batch" || observed.kind === "batch")
                return yield* sessionError("WrongSessionKind");
              const state =
                observed.state === "completed"
                  ? ("completed" as const)
                  : ["completing", "completion-unknown", "initiating"].includes(observed.state)
                    ? ("unknown" as const)
                    : ("failed" as const);
              yield* store.write(
                {
                  ...batch,
                  entries: batch.entries.map((candidate) =>
                    candidate.sessionId === entry.sessionId ? { ...candidate, state } : candidate,
                  ),
                },
                batch.revision,
              );
            }),
          );
        }
        const result = yield* store.exclusive(load(id));
        if (result.kind !== "batch") return yield* sessionError("WrongSessionKind");
        return result.entries;
      }),
    });
  });
  static create<E, R>(request: CreateUpload<E, R>) {
    return Effect.flatMap(UploadSessions, (sessions) => sessions.create(request));
  }
  static resume<E, R>(id: string, source: UploadSource<E, R>) {
    return Effect.flatMap(UploadSessions, (sessions) => sessions.resume(id, source));
  }
  static status(id: string) {
    return Effect.flatMap(UploadSessions, (sessions) => sessions.status(id));
  }
  static reconcile(id: string) {
    return Effect.flatMap(UploadSessions, (sessions) => sessions.reconcile(id));
  }
  static abort(id: string) {
    return Effect.flatMap(UploadSessions, (sessions) => sessions.abort(id));
  }

  static layer(options: SessionOptions) {
    return Layer.effect(UploadSessions, UploadSessions.make(options));
  }
}
