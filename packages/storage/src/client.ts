import { Context, Effect, Layer, Schema } from "effect";

import { UploadClientStore } from "./client/store.js";
import type { ClientUploadStore, UploadTicket } from "./client/store.js";
import { fetchTransfer, providerUrl, xhrTransfer } from "./client/transport.js";
import type { Progress } from "./client/transport.js";
import { sha256 } from "./runtime/portable-hash.js";
import {
  GrantResponse,
  UploadErrorCode,
  UploadInput,
  UploadStatus,
  failUpload,
} from "./uploads/schema.js";
export { UploadClientStore };
export type { ClientUploadStore, UploadTicket } from "./client/store.js";
export type { UploadProgress } from "./client/transport.js";
export interface UploadClientOptions<R = never> {
  readonly baseUrl: string;
  readonly credentials?: RequestCredentials;
  readonly headers?: () => Effect.Effect<
    Readonly<Record<string, string>>,
    ReturnType<typeof failUpload>,
    R
  >;
  readonly store?: ClientUploadStore;
  readonly transport?: "fetch" | "xhr";
  readonly deadlineMillis?: number;
}
const decode = <S extends Schema.Constraint>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => failUpload("BadRequest")));
const hashFile = Effect.fnUntraced(function* (file: File, onProgress?: Progress) {
  const hash = sha256.create();
  for (let offset = 0; offset < file.size; offset += 1024 ** 2) {
    const bytes = yield* Effect.tryPromise({
      try: () => file.slice(offset, offset + 1024 ** 2).arrayBuffer(),
      catch: () => failUpload("Unavailable"),
    });
    hash.update(new Uint8Array(bytes));
    if (onProgress)
      yield* Effect.try({
        try: () =>
          onProgress({
            phase: "hashing",
            bytesAccepted: 0,
            totalBytes: file.size,
            measurement: "indeterminate",
          }),
        catch: () => failUpload("Unavailable"),
      });
  }
  return hash.finish().value;
});
const emit = (onProgress: Progress | undefined, value: Parameters<Progress>[0]) =>
  onProgress
    ? Effect.try({ try: () => onProgress(value), catch: () => failUpload("Unavailable") })
    : Effect.void;

const path = (id: string) => `/uploads/${encodeURIComponent(id)}`;

const successful = (response: { readonly status: number }) =>
  response.status >= 200 && response.status < 300
    ? Effect.void
    : Effect.fail(
        failUpload(response.status === 401 || response.status === 403 ? "Expired" : "Unavailable"),
      );

const make = Effect.fnUntraced(function* <R>(options: UploadClientOptions<R>) {
  const context = yield* Effect.context<R>();
  const base = options.baseUrl.replace(/\/$/u, "");
  const parsed = URL.parse(
    base,
    typeof location === "undefined" ? "http://localhost" : location.href,
  );
  if (
    !parsed ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash ||
    !(
      base.startsWith("/") ||
      parsed.protocol === "https:" ||
      (parsed.protocol === "http:" && ["localhost", "127.0.0.1"].includes(parsed.hostname))
    )
  )
    return yield* failUpload("BadRequest");
  const deadline = options.deadlineMillis ?? 30 * 60 * 1000;
  if (!Number.isSafeInteger(deadline) || deadline <= 0) return yield* failUpload("BadRequest");
  const store = options.store ?? UploadClientStore.memory();
  const api = Effect.fnUntraced(function* <S extends Schema.Constraint>(
    method: string,
    resourcePath: string,
    schema: S,
    body?: unknown,
  ) {
    const headers = options.headers ? yield* Effect.provide(options.headers(), context) : {};
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        fetch(base + resourcePath, {
          method,
          credentials: options.credentials ?? "same-origin",
          redirect: "error",
          headers: {
            ...headers,
            ...(body !== undefined ? { "content-type": "application/json" } : {}),
          },
          ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
          signal,
        }),
      catch: () => failUpload("Unavailable"),
    });
    const value = yield* Effect.tryPromise({
      try: async () => {
        const reader = response.body?.getReader();
        if (!reader) throw new Error("Missing response");
        let text = "";
        const decoder = new TextDecoder();
        let bytes = 0;
        try {
          while (true) {
            // oxlint-disable-next-line no-await-in-loop -- Response bytes must be consumed in order under a size bound.
            const next = await reader.read();
            if (next.done) break;
            bytes += next.value.length;
            if (bytes > 1024 * 1024) throw new Error("Response limit");
            text += decoder.decode(next.value, { stream: true });
          }
          return JSON.parse(text + decoder.decode()) as unknown;
        } finally {
          await reader.cancel();
          reader.releaseLock();
        }
      },
      catch: () => failUpload("Unavailable"),
    });
    if (!response.ok) {
      const error = yield* decode(Schema.Struct({ code: UploadErrorCode }), value);
      return yield* failUpload(error.code);
    }
    return yield* decode(schema, value);
  });
  const ticket = Effect.fnUntraced(function* (id: string, file: File, onProgress?: Progress) {
    const record = yield* store.read(id);
    if (!record || !record.id) return yield* failUpload("NotFound");
    if (
      record.input.file.size !== file.size ||
      record.input.file.name !== file.name ||
      record.input.file.contentType !== (file.type || "application/octet-stream") ||
      record.input.file.sha256 !== (yield* hashFile(file, onProgress))
    )
      return yield* failUpload("Conflict");
    return record;
  });
  const transfer = Effect.fnUntraced(function* (input: {
    readonly uploadId: string;
    readonly file: File;
    readonly onProgress?: Progress;
  }) {
    const record = yield* ticket(input.uploadId, input.file, input.onProgress);
    const id = record.id;
    if (!id) return yield* failUpload("NotFound");
    let grant = (yield* api("POST", `${path(id)}/grants`, GrantResponse)).grant;
    let accepted = 0;
    const send = Effect.fnUntraced(function* (
      url: string,
      method: string,
      headers: Readonly<Record<string, string>>,
      body: Blob | FormData,
      offset: number,
    ) {
      const target = yield* providerUrl(url);
      const safeHeaders = Object.fromEntries(
        Object.entries(headers).filter(
          ([name]) => !["content-length", "host"].includes(name.toLowerCase()),
        ),
      );
      return yield* options.transport === "xhr"
        ? xhrTransfer(target, method, safeHeaders, body, (sent) =>
            input.onProgress?.({
              phase: "uploading",
              bytesSent: offset + sent,
              bytesAccepted: accepted,
              totalBytes: input.file.size,
              measurement: "xhr",
            }),
          )
        : fetchTransfer(target, method, safeHeaders, body);
    });
    const notify = () =>
      emit(input.onProgress, {
        phase: "uploading",
        bytesAccepted: accepted,
        totalBytes: input.file.size,
        measurement: "provider-accepted",
      });
    const parts: Array<{ partNumber: number; receipt: string }> = [];
    yield* emit(input.onProgress, {
      phase: "uploading",
      bytesAccepted: 0,
      totalBytes: input.file.size,
      measurement: "indeterminate",
    });
    if (grant._tag === "Proxy") {
      const headers = options.headers ? yield* Effect.provide(options.headers(), context) : {};
      const response = yield* Effect.tryPromise({
        try: (signal) =>
          fetch(`${base}${path(id)}/content`, {
            method: "PUT",
            credentials: options.credentials ?? "same-origin",
            headers: { ...headers, "content-type": input.file.type || "application/octet-stream" },
            body: input.file,
            signal,
            redirect: "error",
          }),
        catch: () => failUpload("Unavailable"),
      });
      yield* successful(response);
      accepted = input.file.size;
    } else if (grant._tag === "SignedRequest") {
      yield* successful(yield* send(grant.url, grant.method, grant.headers, input.file, 0));
      accepted = input.file.size;
    } else if (grant._tag === "PostForm") {
      const form = new FormData();
      for (const [key, value] of Object.entries(grant.fields)) form.append(key, value);
      form.append("file", input.file);
      yield* successful(yield* send(grant.url, "POST", {}, form, 0));
      accepted = input.file.size;
    } else if (grant._tag === "Multipart" || grant._tag === "Blocks") {
      const count = Math.ceil(input.file.size / grant.partBytes);
      if (count > 10000 || count < 1) return yield* failUpload("BadRequest");
      for (let partNumber = 1; partNumber <= count; partNumber++) {
        const offset = (partNumber - 1) * grant.partBytes;
        const size = Math.min(grant.partBytes, input.file.size - offset);
        const done =
          grant._tag === "Multipart"
            ? grant.completed.find((part) => part.partNumber === partNumber)
            : undefined;
        if (done) {
          parts.push({ partNumber: done.partNumber, receipt: done.etag });
          accepted += size;
          continue;
        }
        const selected = (yield* api("POST", `${path(id)}/parts/sign`, GrantResponse, {
          partNumber,
        })).grant;
        if (selected._tag !== "SignedRequest") return yield* failUpload("BadRequest");
        const response = yield* send(
          selected.url,
          selected.method,
          selected.headers,
          input.file.slice(offset, offset + size),
          offset,
        );
        yield* successful(response);
        const etag = selected.partReceipt ?? response.headers.get("etag");
        if (!etag) return yield* failUpload("Unsupported");
        parts.push({ partNumber, receipt: etag });
        accepted += size;
        yield* notify();
      }
    } else {
      if (grant.offset > input.file.size) return yield* failUpload("BadRequest");
      accepted = grant.offset;
      for (let attempt = 0; accepted < input.file.size && attempt < 100000; attempt++) {
        const end = Math.min(accepted + grant.chunkBytes, input.file.size);
        const headers =
          grant.protocol === "gcs"
            ? {
                ...grant.headers,
                "content-range": `bytes ${accepted}-${end - 1}/${input.file.size}`,
              }
            : {
                ...grant.headers,
                "tus-resumable": "1.0.0",
                "upload-offset": String(accepted),
                "content-type": "application/offset+octet-stream",
              };
        const response = yield* send(
          grant.url,
          grant.protocol === "gcs" ? "PUT" : "PATCH",
          headers,
          input.file.slice(accepted, end),
          accepted,
        );
        if (response.status !== 308) yield* successful(response);
        const observed = (yield* api("POST", `${path(id)}/grants`, GrantResponse)).grant;
        if (observed._tag !== "Resumable" || observed.offset <= accepted || observed.offset > end)
          return yield* failUpload("Unavailable");
        accepted = observed.offset;
        grant = observed;
        yield* notify();
      }
      if (accepted !== input.file.size) return yield* failUpload("Unavailable");
    }
    yield* notify();
    return { uploadId: id, parts };
  });
  const complete = Effect.fnUntraced(function* (input: {
    readonly uploadId: string;
    readonly parts?: ReadonlyArray<{ readonly partNumber: number; readonly receipt: string }>;
    readonly onProgress?: Progress;
  }) {
    const result = yield* api(
      "POST",
      `${path(input.uploadId)}/complete`,
      UploadStatus,
      input.parts?.length ? { parts: input.parts } : {},
    );
    if (result.state === "published") {
      yield* store.remove(input.uploadId);
      yield* emit(input.onProgress, {
        phase: "completed",
        bytesAccepted: 0,
        totalBytes: 0,
        measurement: "provider-accepted",
      });
    }
    return result;
  });
  const create = Effect.fnUntraced(function* (input: {
    readonly file: File;
    readonly purpose: string;
    readonly context: Readonly<Record<string, string>>;
    readonly strategy?: UploadInput["strategy"];
    readonly idempotencyKey?: string;
    readonly onProgress?: Progress;
  }) {
    const metadata = yield* decode(UploadInput, {
      purpose: input.purpose,
      file: {
        name: input.file.name,
        size: input.file.size,
        contentType: input.file.type || "application/octet-stream",
        sha256: yield* hashFile(input.file, input.onProgress),
      },
      context: input.context,
      ...(input.strategy ? { strategy: input.strategy } : {}),
    });
    const key = input.idempotencyKey ?? crypto.randomUUID();
    const local: UploadTicket = { version: 1, idempotencyKey: key, input: metadata };
    yield* store.write(key, local);
    const status = yield* api("POST", "/uploads", UploadStatus, {
      version: 1,
      idempotencyKey: key,
      input: metadata,
    });
    const saved = { ...local, id: status.id };
    yield* store.write(key, saved);
    yield* store.write(status.id, saved);
    return saved;
  });
  return {
    create,
    transfer,
    complete,
    status: (input: { readonly uploadId: string }) =>
      api("GET", path(input.uploadId), UploadStatus),
    abort: (input: { readonly uploadId: string }) =>
      api("DELETE", path(input.uploadId), UploadStatus),
    reconcile: (input: { readonly uploadId: string }) =>
      api("POST", `${path(input.uploadId)}/reconcile`, UploadStatus),
    upload: (input: Parameters<typeof create>[0]) =>
      Effect.gen(function* () {
        const saved = yield* create(input);
        const sent = yield* transfer({
          uploadId: saved.id,
          file: input.file,
          ...(input.onProgress ? { onProgress: input.onProgress } : {}),
        });
        yield* emit(input.onProgress, {
          phase: "verifying",
          bytesAccepted: input.file.size,
          totalBytes: input.file.size,
          measurement: "provider-accepted",
        });
        return yield* complete({
          ...sent,
          ...(input.onProgress ? { onProgress: input.onProgress } : {}),
        });
      }).pipe(
        Effect.timeoutOrElse({
          duration: deadline,
          orElse: () => Effect.fail(failUpload("Unavailable")),
        }),
      ),
    resume: (input: {
      readonly uploadId: string;
      readonly file: File;
      readonly onProgress?: Progress;
    }) =>
      Effect.gen(function* () {
        const record = yield* store.read(input.uploadId);
        if (!record) return yield* failUpload("NotFound");
        if (
          record.input.file.size !== input.file.size ||
          record.input.file.name !== input.file.name ||
          record.input.file.contentType !== (input.file.type || "application/octet-stream") ||
          record.input.file.sha256 !== (yield* hashFile(input.file, input.onProgress))
        )
          return yield* failUpload("Conflict");
        if (!record.id)
          yield* create({
            file: input.file,
            purpose: record.input.purpose,
            context: record.input.context,
            idempotencyKey: record.idempotencyKey,
            ...(record.input.strategy ? { strategy: record.input.strategy } : {}),
          });
        const saved = yield* store.read(input.uploadId);
        if (!saved?.id) return yield* failUpload("NotFound");
        yield* ticket(saved.id, input.file, input.onProgress);
        const status = yield* api("GET", path(saved.id), UploadStatus);
        if (status.state === "published") return status;
        if (["uploading", "verifying", "publishing"].includes(status.state)) {
          const previous = yield* Effect.result(
            complete({
              uploadId: saved.id,
              ...(input.onProgress ? { onProgress: input.onProgress } : {}),
            }),
          );
          if (previous._tag === "Success") return previous.success;
          if (status.state !== "uploading" || previous.failure.code !== "Unavailable")
            return yield* previous.failure;
        }
        const sent = yield* transfer({ ...input, uploadId: saved.id });
        return yield* complete({
          ...sent,
          ...(input.onProgress ? { onProgress: input.onProgress } : {}),
        });
      }).pipe(
        Effect.timeoutOrElse({
          duration: deadline,
          orElse: () => Effect.fail(failUpload("Unavailable")),
        }),
      ),
  };
});
export class UploadClient extends Context.Service<
  UploadClient,
  Effect.Success<ReturnType<typeof make>>
>()("@effect-kit/storage/UploadClient") {
  static readonly make = make;
  static layer<R>(options: UploadClientOptions<R>) {
    return Layer.effect(UploadClient, make(options));
  }
}
