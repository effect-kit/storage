import { Context, Effect, Layer, Redacted, Result, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { Storage } from "@effect-kit/storage";
import * as WebPlatform from "@effect-kit/storage/browser";
import { R2WorkersStorage } from "@effect-kit/storage/r2-workers";
import { UploadSessions, UploadSessionStore } from "@effect-kit/storage/sessions";
import { runStorageConformance } from "@effect-kit/storage/testing";
import { TransferDriver, Transfers, UploadSource } from "@effect-kit/storage/transfers";

const check = (condition, message) => {
  if (!condition) throw new Error(message);
};
const program = (bucket) =>
  Effect.scoped(
    Effect.gen(function* () {
      const conformance = yield* runStorageConformance(() =>
        Effect.gen(function* () {
          let cursor;
          do {
            const page = yield* Effect.promise(() => bucket.list({ cursor }));
            for (const object of page.objects)
              yield* Effect.promise(() => bucket.delete(object.key));
            cursor = page.truncated ? page.cursor : undefined;
          } while (cursor);
          return yield* R2WorkersStorage.make({ bucket, storeId: "conformance" });
        }),
      );
      const checks = ["portable storage conformance"];
      const context = yield* Layer.build(
        Transfers.layer({
          maxBytes: 16 * 1024 * 1024,
          partBytes: 5 * 1024 * 1024,
          partConcurrency: 1,
          memoryBudgetBytes: 32 * 1024 * 1024,
          maxSourceChunkBytes: 65536,
        }).pipe(
          Layer.provideMerge(R2WorkersStorage.layer({ bucket, storeId: "worker-acceptance" })),
        ),
      );
      const storage = Context.get(context, Storage);
      const native = Context.get(context, R2WorkersStorage);
      const driver = Context.get(context, TransferDriver);
      const transfers = Context.get(context, Transfers);
      check(
        driver.storage === storage && !driver.handles,
        "driver identity and explicit durable upload limit",
      );

      const known = yield* storage.put({
        key: "known-stream",
        contentLength: 3,
        body: Stream.make(new Uint8Array([1]), new Uint8Array([2, 3])),
      });
      check(known.size === 3, "known-length stream size");
      const bytes = yield* Storage.getBytes({ key: "known-stream", maxBytes: 3 }).pipe(
        Effect.provideService(Storage, storage),
      );
      check([...bytes].join(",") === "1,2,3", "known-length streamed bytes");
      const rejectedStream = yield* storage
        .put({
          key: "known-stream",
          contentLength: 3,
          body: Stream.make(new Uint8Array([4]), new Uint8Array([5, 6])),
          condition: { _tag: "IfMatch", etag: '"stale"' },
        })
        .pipe(Effect.result);
      check(
        Result.isFailure(rejectedStream) &&
          rejectedStream.failure.reason._tag === "PreconditionFailed",
        "rejected conditional stream joins its producer",
      );
      const sourceError = { _tag: "ApplicationSource" };
      const sourceFailure = yield* storage
        .put({
          key: "failed-source",
          contentLength: 3,
          body: Stream.concat(Stream.succeed(new Uint8Array([1])), Stream.fail(sourceError)),
        })
        .pipe(Effect.result);
      check(
        Result.isFailure(sourceFailure) && sourceFailure.failure === sourceError,
        "source error preserved",
      );
      const short = yield* storage
        .put({ key: "short-source", contentLength: 3, body: Stream.succeed(new Uint8Array([1])) })
        .pipe(Effect.result);
      check(
        Result.isFailure(short) && short.failure.reason._tag === "LengthMismatch",
        "short source rejected",
      );
      checks.push("native FixedLengthStream backpressure, length checks and source errors");

      const first = yield* native.multipart.initiate({
        key: "native-multipart",
        headers: { customMetadata: { profile: "native" } },
      });
      const part1 = yield* native.multipart.uploadPart({
        upload: first,
        partNumber: 1,
        body: new Uint8Array(5 * 1024 * 1024).fill(8),
        contentLength: 5 * 1024 * 1024,
      });
      const retriedPart = yield* native.multipart.uploadPart({
        upload: first,
        partNumber: 1,
        body: new Uint8Array(5 * 1024 * 1024).fill(8),
        contentLength: 5 * 1024 * 1024,
      });
      check(retriedPart.partNumber === part1.partNumber, "same-number stable-byte part retry");
      const part2 = yield* native.multipart.uploadPart({
        upload: first,
        partNumber: 2,
        body: new Uint8Array([9]),
        contentLength: 1,
      });
      const completed = yield* native.multipart.complete({
        upload: first,
        parts: [retriedPart, part2],
      });
      check(completed.size === 5 * 1024 * 1024 + 1, "native multipart size");
      const abort = yield* native.multipart.initiate({ key: "aborted" });
      yield* native.multipart.abort(abort);
      const invalid = {
        ref: { storeId: storage.storeId, key: "missing-upload" },
        uploadId: Redacted.make("invalid-upload-id"),
      };
      const invalidPart = yield* native.multipart
        .uploadPart({ upload: invalid, partNumber: 1, body: new Uint8Array([1]), contentLength: 1 })
        .pipe(Effect.result);
      check(Result.isFailure(invalidPart), "resume handle does not prove existence");
      const inventory = yield* native.multipart.listParts({ upload: first }).pipe(Effect.result);
      check(
        Result.isFailure(inventory) && inventory.failure.reason._tag === "Unsupported",
        "no invented inventory",
      );
      checks.push("native multipart create/part/complete/abort and missing-inventory limits");

      const receipt = yield* transfers.upload({
        key: "managed-multipart",
        strategy: "multipart",
        source: UploadSource.bytes(new Uint8Array(5 * 1024 * 1024 + 17).fill(4)),
      });
      check(receipt.metadata.size === 5 * 1024 * 1024 + 17, "managed multipart size");
      const downloaded = yield* transfers.download({
        ref: receipt.metadata.ref,
        expectedChecksum: receipt.checksum,
        target: {
          write: (chunk) =>
            Effect.sync(() =>
              check(
                chunk.every((byte) => byte === 4),
                "managed byte integrity",
              ),
            ),
        },
      });
      check(downloaded.checksum.value === receipt.checksum.value, "managed full checksum");
      let opened = false;
      const conditional = yield* transfers
        .upload({
          key: "conditional-multipart",
          strategy: "multipart",
          condition: { _tag: "CreateOnly" },
          source: UploadSource.replayable(() => {
            opened = true;
            return Stream.empty;
          }, 0),
        })
        .pipe(Effect.result);
      check(
        Result.isFailure(conditional) &&
          conditional.failure.reason._tag === "Unsupported" &&
          !opened,
        "conditional multipart rejected before source opening",
      );
      checks.push("managed multipart and pinned full-checksum download with bounded admission");

      const copied = yield* transfers.copy({
        source: driver,
        ref: known.ref,
        destinationKey: "streamed-copy",
      });
      check(
        copied.strategy === "streamed-copy" && copied.progress.bytesVerified === 3,
        "verified streamed copy",
      );
      const moved = yield* transfers
        .move({ source: driver, ref: copied.metadata.ref, destinationKey: "move-copy" })
        .pipe(Effect.result);
      check(
        Result.isFailure(moved) && moved.failure.reason._tag === "Unsupported",
        "move requires pinned deletion authority",
      );
      check(
        (yield* storage.head({ ref: copied.metadata.ref })).size === 3,
        "unsupported move preserves source",
      );
      const absent = yield* storage.head({ key: "move-copy" }).pipe(Effect.result);
      check(
        Result.isFailure(absent) && absent.failure.reason._tag === "NotFound",
        "unsupported move rejects before destination mutation",
      );
      checks.push("verified copy fallback and early unsupported move without mutation");

      const protection = yield* WebPlatform.aes256Gcm(
        Redacted.make(crypto.getRandomValues(new Uint8Array(32))),
      );
      const store = yield* UploadSessionStore.make({
        namespace: "worker",
        ownership: "exclusive",
        protection,
      }).pipe(Effect.provide(KeyValueStore.layerMemory));
      const sessions = yield* UploadSessions.make({ configurationId: "worker-profile-v1" }).pipe(
        Effect.provide(context),
        Effect.provideService(UploadSessionStore, store),
      );
      const unsupported = yield* sessions
        .create({ key: "durable-unsupported", source: UploadSource.bytes(new Uint8Array([1])) })
        .pipe(Effect.result);
      check(Result.isFailure(unsupported), "durable multipart upload must fail closed");
      const downloadId = yield* sessions.createDownload({ id: "download", ref: known.ref });
      const output = [];
      yield* sessions.resumeDownload(downloadId, {
        id: "download",
        prefix: (length) => Stream.succeed(Uint8Array.from(output.slice(0, length))),
        truncate: (length) =>
          Effect.sync(() => {
            output.length = length;
          }),
        write: (offset, chunk) =>
          Effect.sync(() => {
            for (let index = 0; index < chunk.length; index++)
              output[offset + index] = chunk[index];
          }),
        sync: Effect.void,
        publish: () => Effect.void,
      });
      check(
        output.join(",") === "1,2,3" && (yield* sessions.status(downloadId)).state === "completed",
        "protected download session",
      );
      checks.push("Worker crypto and protected memory-backed download session");
      return { checks, conformance, resources: yield* transfers.resources };
    }),
  ).pipe(Effect.provide(WebPlatform.layer));

export default {
  async fetch(_request, env) {
    if (env.FIXTURE_PROFILE !== "owned-local")
      return new Response("Local fixture only", { status: 400 });
    try {
      return Response.json({ status: "passed", ...(await Effect.runPromise(program(env.BUCKET))) });
    } catch (error) {
      return Response.json(
        {
          status: "failed",
          message: String(error),
          testName: error?.testName,
          detail: error?.detail,
          reason: error?.reason,
          stack: error?.stack,
        },
        { status: 500 },
      );
    }
  },
};
