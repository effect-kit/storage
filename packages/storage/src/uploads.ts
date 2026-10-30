import { Clock, Context, Effect, Layer, Redacted, Schema } from "effect";

import { StorageEvent } from "./events.js";
import type { UploadBody } from "./requests.js";
import { Storage } from "./storage.js";
import { UploadGrantDriver } from "./uploads/driver.js";
import type { UploadOptions, UploadAction } from "./uploads/policy.js";
import {
  UploadActor,
  UploadRequest,
  UploadId,
  PartRequest,
  PartsRequest,
  failUpload,
} from "./uploads/schema.js";
import type { UploadRecord, UploadStatus, UploadInput } from "./uploads/schema.js";
import { UploadStore } from "./uploads/store.js";
import { cleanup, digestText, snapshot } from "./uploads/verification.js";
export * from "./uploads/schema.js";
export { UploadStore, UploadGrantDriver };
export { SignedUploadGrants } from "./uploads/signed-driver.js";
export type { UploadStoreOptions } from "./uploads/store.js";
export type { UploadOptions, UploadDecision, UploadAction } from "./uploads/policy.js";
export type { UploadGrantDriverService } from "./uploads/driver.js";
const decode = <S extends Schema.Constraint>(schema: S, input: unknown) =>
  Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() => failUpload("BadRequest")),
  );
const status = (record: UploadRecord): UploadStatus => ({
  version: 1,
  id: record.id,
  state: record.state,
  expiresAt: record.expiresAt,
  cleanupPending: record.cleanupPending,
  ...(record.state === "published" ? { result: record.result } : {}),
});
const make = Effect.fnUntraced(function* <R>(options: UploadOptions<R>) {
  if (
    options.quarantine !== "private" ||
    !Number.isSafeInteger(options.grantSeconds ?? 900) ||
    (options.grantSeconds ?? 900) < 1 ||
    (options.grantSeconds ?? 900) > 3600
  )
    return yield* failUpload("BadRequest");
  const context = yield* Effect.context<R>();
  const storage = yield* Storage;
  const store = yield* UploadStore;
  const driver = yield* UploadGrantDriver;
  const hook = <A, E>(program: Effect.Effect<A, E, R>) => Effect.provide(program, context);
  const allowed = Effect.fnUntraced(function* (
    actor: UploadActor,
    record: UploadRecord,
    action: UploadAction,
  ) {
    const decision = yield* hook(
      options.authorize({ actor, input: record.input, uploadId: record.id, action }),
    );
    if (
      decision.key !== record.key ||
      !Number.isSafeInteger(decision.maxBytes) ||
      record.input.file.size > decision.maxBytes ||
      !decision.allowedContentTypes.includes(record.input.file.contentType)
    )
      return yield* failUpload("Forbidden");
    return decision;
  });
  const load = Effect.fnUntraced(function* (
    raw: { readonly actor: UploadActor; readonly uploadId: string },
    action: UploadAction,
  ) {
    const actor = yield* decode(UploadActor, raw.actor);
    const id = yield* decode(UploadId, raw.uploadId);
    const record = yield* store.read(id);
    if (
      !record ||
      record.storeId !== storage.storeId ||
      record.actor.subject !== actor.subject ||
      record.actor.tenantId !== actor.tenantId
    )
      return yield* failUpload("NotFound");
    yield* allowed(actor, record, action);
    return record;
  });
  const active = Effect.fnUntraced(function* (record: UploadRecord) {
    if (record.expiresAt <= (yield* Clock.currentTimeMillis)) return yield* failUpload("Expired");
    if (!["ready", "uploading"].includes(record.state)) return yield* failUpload("Conflict");
  });
  const grant = Effect.fnUntraced(function* (input: {
    readonly actor: UploadActor;
    readonly uploadId: string;
    readonly partNumber?: number;
  }) {
    const record = yield* load(input, "grant");
    yield* active(record);
    const now = yield* Clock.currentTimeMillis;
    const expiresAt = driver.maximumGrantLifetimeMillis
      ? now + driver.maximumGrantLifetimeMillis
      : Math.min(record.expiresAt, now + (options.grantSeconds ?? 900) * 1000);
    // Persist the possible grant lifetime before issuing bearer authority.
    let issuing = {
      ...record,
      grantExpiresAt: Math.max(record.grantExpiresAt, expiresAt),
      cleanupPending: true,
    };
    yield* store.write(issuing);
    if (record.remote === undefined) {
      yield* store.write({ ...issuing, state: "initiating" });
      const remote = yield* driver.begin(issuing);
      issuing = { ...issuing, remote: Redacted.make(remote), state: "uploading" };
      yield* store.write(issuing);
    }
    const native =
      input.partNumber !== undefined
        ? yield* driver.part
            ? driver.part(
                issuing,
                (yield* decode(PartRequest, { partNumber: input.partNumber })).partNumber,
                expiresAt,
              )
            : Effect.fail(failUpload("Unsupported"))
        : yield* driver.grant(issuing, expiresAt);
    if (native.expiresAt > expiresAt) return yield* failUpload("Unsupported");
    return {
      version: 1 as const,
      id: record.id,
      grant: Redacted.make({
        ...native,
        constraints: [
          ...(native.constraints ?? []),
          { kind: "size", enforcement: "backend" as const, value: record.input.file.size },
          { kind: "sha256", enforcement: "backend" as const, value: record.input.file.sha256 },
          {
            kind: "declared-content-type",
            enforcement: "advisory" as const,
            value: record.input.file.contentType,
          },
          {
            kind: "content-inspection",
            enforcement: options.inspect ? ("backend" as const) : ("advisory" as const),
            value: options.inspect ? "required" : "not-configured",
          },
        ],
      }),
    };
  });
  const complete = Effect.fnUntraced(function* (input: {
    readonly actor: UploadActor;
    readonly uploadId: string;
    readonly parts?: (typeof PartsRequest.Type)["parts"];
  }) {
    let record = yield* load(input, "complete");
    if (record.state === "published") return status(record);
    if (["aborted", "rejected", "reserving", "initiating"].includes(record.state))
      return yield* failUpload("Conflict");
    if (
      record.expiresAt <= (yield* Clock.currentTimeMillis) &&
      !["publishing", "verifying"].includes(record.state)
    )
      return yield* failUpload("Expired");
    if (input.parts && driver.complete && !record.object) {
      const committed = yield* storage
        .head({ key: record.stagingKey })
        .pipe(Effect.catchReason("StorageError", "NotFound", () => Effect.succeed(undefined)));
      if (!committed)
        yield* driver.complete(record, (yield* decode(PartsRequest, { parts: input.parts })).parts);
    }
    if (!record.object) {
      if (record.state !== "verifying") yield* storage.head({ key: record.stagingKey });
      record = { ...record, state: "verifying" };
      yield* store.write(record);
      const outcome = yield* Effect.result(snapshot(storage, record));
      if (outcome._tag === "Failure") {
        if (outcome.failure._tag === "UploadError" && outcome.failure.code === "VerificationFailed")
          yield* store.write({ ...record, state: "rejected" });
        return yield* outcome.failure;
      }
      record = { ...record, object: outcome.success };
      yield* store.write(record);
    }
    const object = record.object;
    if (!object) return yield* failUpload("Unavailable");
    if (record.state !== "publishing" && options.inspect)
      yield* hook(options.inspect({ object, file: record.input.file }));
    record = { ...record, state: "publishing" };
    yield* store.write(record);
    if (options.quota) yield* hook(options.quota.settle(record));
    const result = yield* hook(
      options.publish({ idempotencyKey: record.id, object, context: record.context }),
    );
    record = { ...record, state: "published", result };
    yield* store.write(record);
    return status(record);
  });
  const reconcile = Effect.fnUntraced(function* (input: {
    readonly actor: UploadActor;
    readonly uploadId: string;
  }) {
    let record = yield* load(input, "reconcile");
    if (
      !["published", "publishing", "verifying", "aborted", "rejected"].includes(record.state) &&
      record.expiresAt <= (yield* Clock.currentTimeMillis)
    ) {
      record = { ...record, state: "aborted", cleanupPending: true };
      yield* store.write(record);
      yield* driver.abort(record);
      if (options.quota) yield* hook(options.quota.release(record));
    }
    if (["verifying", "publishing"].includes(record.state)) return yield* complete(input);
    if (
      record.cleanupPending &&
      record.grantExpiresAt <= (yield* Clock.currentTimeMillis) &&
      ["aborted", "published", "rejected"].includes(record.state)
    ) {
      const cleaned = yield* cleanup(storage, record);
      record = {
        ...record,
        cleanupPending: !cleaned || (!driver.confirmsTermination && record.grantExpiresAt > 0),
      };
      yield* store.write(record);
    }
    return status(record);
  });
  return {
    create: (raw: {
      readonly actor: UploadActor;
      readonly idempotencyKey: string;
      readonly input: UploadInput;
    }) =>
      store.exclusive(
        Effect.gen(function* () {
          const actor = yield* decode(UploadActor, raw.actor);
          const request = yield* decode(UploadRequest, {
            version: 1,
            input: raw.input,
            idempotencyKey: raw.idempotencyKey,
          });
          const id = digestText(
            JSON.stringify([
              actor.subject,
              actor.tenantId,
              storage.storeId,
              request.idempotencyKey,
            ]),
          );
          let record = yield* store.read(id);
          if (record) {
            if (
              digestText(JSON.stringify(record.input)) !== digestText(JSON.stringify(request.input))
            )
              return yield* failUpload("Conflict");
            yield* allowed(actor, record, "create");
          } else {
            const decision = yield* hook(
              options.authorize({ actor, input: request.input, uploadId: id, action: "create" }),
            );
            if (
              !Number.isSafeInteger(decision.expiresInSeconds) ||
              decision.expiresInSeconds < 1 ||
              decision.expiresInSeconds > 86400 ||
              !decision.key ||
              !Number.isSafeInteger(decision.maxBytes) ||
              request.input.file.size > decision.maxBytes ||
              !decision.allowedContentTypes.includes(request.input.file.contentType)
            )
              return yield* failUpload("Forbidden");
            record = {
              version: 1,
              id,
              actor,
              input: request.input,
              storeId: storage.storeId,
              key: decision.key,
              stagingKey: `${decision.key}.upload-${id}`,
              publishedKey: `${decision.key}.object-${id}`,
              context: decision.context,
              expiresAt: (yield* Clock.currentTimeMillis) + decision.expiresInSeconds * 1000,
              grantExpiresAt: 0,
              state: "reserving",
              cleanupPending: false,
            };
            yield* store.write(record);
          }
          if (record.state === "reserving") {
            if (options.quota) yield* hook(options.quota.reserve(record));
            record = { ...record, state: "ready" };
            yield* store.write(record);
          }
          return status(record);
        }),
      ),
    status: (input: { readonly actor: UploadActor; readonly uploadId: string }) =>
      store.exclusive(load(input, "status").pipe(Effect.map(status))),
    grant: (input: { readonly actor: UploadActor; readonly uploadId: string }) =>
      store.exclusive(grant(input)),
    part: (input: {
      readonly actor: UploadActor;
      readonly uploadId: string;
      readonly partNumber: number;
    }) => store.exclusive(grant(input)),
    complete: (input: {
      readonly actor: UploadActor;
      readonly uploadId: string;
      readonly parts?: (typeof PartsRequest.Type)["parts"];
    }) => store.exclusive(complete(input)),
    reconcile: (input: { readonly actor: UploadActor; readonly uploadId: string }) =>
      store.exclusive(reconcile(input)),
    observe: (input: {
      readonly actor: UploadActor;
      readonly uploadId: string;
      readonly event: StorageEvent;
    }) =>
      store.exclusive(
        Effect.gen(function* () {
          const record = yield* load(input, "complete");
          const event = yield* decode(StorageEvent, input.event);
          if (
            event.provider !== storage.provider ||
            event.ref.storeId !== storage.storeId ||
            event.ref.key !== record.stagingKey
          )
            return yield* failUpload("Forbidden");
          if (!["object-created", "restore-completed"].includes(event.kind)) return status(record);
          return yield* complete(input);
        }),
      ),
    abort: (input: { readonly actor: UploadActor; readonly uploadId: string }) =>
      store.exclusive(
        Effect.gen(function* () {
          const record = yield* load(input, "abort");
          if (["published", "publishing"].includes(record.state))
            return yield* failUpload("Conflict");
          const aborted = { ...record, state: "aborted" as const, cleanupPending: true };
          yield* store.write(aborted);
          yield* driver.abort(aborted);
          if (options.quota) yield* hook(options.quota.release(aborted));
          return status(aborted);
        }),
      ),
    content: <E = never, ER = never>(input: {
      readonly actor: UploadActor;
      readonly uploadId: string;
      readonly body: UploadBody<E, ER>;
    }) =>
      store.exclusive(
        Effect.gen(function* () {
          const record = yield* load(input, "content");
          yield* active(record);
          if (record.input.strategy && !["auto", "proxy"].includes(record.input.strategy))
            return yield* failUpload("Conflict");
          yield* store.write({ ...record, state: "uploading", cleanupPending: true });
          yield* storage.put({
            key: record.stagingKey,
            body: input.body,
            contentLength: record.input.file.size,
            maxBytes: record.input.file.size,
            contentType: record.input.file.contentType,
            condition: { _tag: "CreateOnly" },
          });
          yield* store.write({ ...record, state: "uploading", cleanupPending: true });
          return status({ ...record, state: "uploading", cleanupPending: true });
        }),
      ),
  };
});
export class Uploads extends Context.Service<Uploads, Effect.Success<ReturnType<typeof make>>>()(
  "@effect-kit/storage/Uploads",
) {
  static readonly make = make;
  static layer<R>(options: UploadOptions<R>) {
    return Layer.effect(Uploads, make(options));
  }
}
