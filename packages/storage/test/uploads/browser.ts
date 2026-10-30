import { Deferred, Effect, Fiber } from "effect";

import { UploadClient, UploadClientStore } from "@effect-kit/storage/client";
import type { UploadProgress } from "@effect-kit/storage/client";
import type { UploadInput } from "@effect-kit/storage/http/schema";

export const makeClient = (provider: string, transport: "fetch" | "xhr" = "fetch") =>
  UploadClient.make({
    baseUrl: `/storage-${provider}`,
    transport,
    store: UploadClientStore.webStorage(localStorage, "acceptance"),
    headers: () => Effect.succeed({ authorization: "Bearer fixture-user" }),
  });
const file = (size: number, changed = false) =>
  new File([new Uint8Array(size).fill(changed ? 2 : 1)], "sample.bin", {
    type: "application/octet-stream",
  });
export const upload = (
  provider: string,
  strategy: UploadInput["strategy"],
  size: number,
  transport: "fetch" | "xhr" = "fetch",
  purpose = "attachment",
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* makeClient(provider, transport);
      const progress: UploadProgress[] = [];
      const result = yield* client.upload({
        file: file(size),
        purpose,
        context: {},
        ...(strategy ? { strategy } : {}),
        onProgress: (next) => {
          progress.push(next);
        },
      });
      return {
        state: result.state,
        result: result.result,
        observedSent: progress.some((event) => (event.bytesSent ?? 0) > 0),
        verified: progress.some((event) => event.phase === "verifying"),
        checkpoints: Object.keys(localStorage).length,
      };
    }),
  );
export const prepare = (provider: string, strategy: UploadInput["strategy"], size: number) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* makeClient(provider);
      const created = yield* client.create({
        file: file(size),
        purpose: "attachment",
        context: {},
        ...(strategy ? { strategy } : {}),
      });
      const forged = yield* Effect.tryPromise(() =>
        fetch(`/storage-${provider}/uploads/${created.id}/complete`, {
          method: "POST",
          headers: { authorization: "Bearer fixture-user", "content-type": "application/json" },
          body: JSON.stringify({}),
        }),
      );
      return { id: created.id, prematureStatus: forged.status };
    }),
  );
export const resume = (provider: string, id: string, size: number, changed = false) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* makeClient(provider);
      const result = yield* Effect.result(
        client.resume({ uploadId: id, file: file(size, changed) }),
      );
      return result._tag === "Success"
        ? { state: result.success.state, code: "" }
        : { state: "failed", code: result.failure.code };
    }),
  );
export const interrupt = (provider: string, strategy: UploadInput["strategy"], size: number) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* makeClient(provider, "xhr");
      const created = yield* client.create({
        file: file(size),
        purpose: "attachment",
        context: {},
        ...(strategy ? { strategy } : {}),
      });
      const started = yield* Deferred.make<void>();
      const fiber = yield* Effect.forkChild(
        client.transfer({
          uploadId: created.id,
          file: file(size),
          onProgress: (event) => {
            if ((event.bytesSent ?? 0) > 0) Effect.runSync(Deferred.succeed(started, undefined));
          },
        }),
      );
      yield* Deferred.await(started).pipe(Effect.timeout("30 seconds"));
      yield* Fiber.interrupt(fiber);
      return { id: created.id };
    }),
  );
export const transferred = (
  provider: string,
  strategy: UploadInput["strategy"],
  size: number,
  purpose = "attachment",
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* makeClient(provider);
      const created = yield* client.create({
        file: file(size),
        purpose,
        context: {},
        ...(strategy ? { strategy } : {}),
      });
      const sent = yield* client.transfer({ uploadId: created.id, file: file(size) });
      return { id: created.id, parts: sent.parts };
    }),
  );
const diagnosticUpload: typeof upload = (...args) =>
  upload(...args).catch((error: unknown) => {
    const code =
      error && typeof error === "object" && "code" in error ? String(error.code) : "Unknown";
    throw new Error(`Upload acceptance failed: ${code}`);
  });
Reflect.set(globalThis, "storageUploadAcceptance", {
  upload: diagnosticUpload,
  prepare,
  resume,
  interrupt,
  transferred,
});
