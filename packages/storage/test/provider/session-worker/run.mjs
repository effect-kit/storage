import { appendFile, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Context, Effect, Layer, Redacted, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { Storage, StorageError } from "../../../dist/index.js";
import { atomicFileStore, aes256Gcm, fileDownloadTarget } from "../../../dist/node.js";
import { layer as platformLayer } from "../../../dist/node.js";
import { S3Storage } from "../../../dist/s3.js";
import { UploadSessions, UploadSessionStore } from "../../../dist/sessions.js";
import { TransferDriver, Transfers, UploadSource } from "../../../dist/transfers.js";

const [directory, key, boundary, mode = "upload"] = process.argv.slice(2);
const secret = process.env.STORAGE_SESSION_TEST_KEY;
if (!directory || !key || !boundary || !secret) throw new Error("Missing worker configuration");
const crash = () => process.exit(73);
const guard = (name, effect) =>
  Effect.gen(function* () {
    if (boundary === `${name}-before`) crash();
    const result = yield* effect;
    if (boundary === `${name}-after`) crash();
    return result;
  });
const size = 6 * 1024 * 1024 + 3;
const source = UploadSource.replayable(
  () =>
    Stream.fromIterable(
      (function* () {
        for (let offset = 0; offset < size; offset += 65536)
          yield new Uint8Array(Math.min(65536, size - offset)).fill(19);
      })(),
      { chunkSize: 1 },
    ),
  size,
);

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const provider = yield* Layer.build(
        S3Storage.layer({
          bucket: process.env.STORAGE_S3_TEST_BUCKET,
          region: process.env.STORAGE_S3_TEST_REGION,
          endpoint: process.env.STORAGE_S3_TEST_ENDPOINT,
          forcePathStyle: true,
        }),
      );
      const storage = Context.get(provider, S3Storage);
      const endpoint = Context.get(provider, TransferDriver);
      const mpu = endpoint.multipart;
      let downloadWrites = 0;
      const target = yield* fileDownloadTarget({
        id: "download",
        path: join(directory, "downloaded.bin"),
      });
      const base = yield* atomicFileStore(directory);
      const protection = aes256Gcm(Redacted.make(Buffer.from(secret, "base64")));
      let savedState;
      let savedParts = 0;
      const store = yield* UploadSessionStore.make({
        namespace: "restart",
        ownership: "exclusive",
        protection: {
          ...protection,
          seal: (plaintext, aad) =>
            protection.seal(plaintext, aad).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  const checkpoint = JSON.parse(plaintext);
                  savedState = checkpoint.state;
                  savedParts = checkpoint.parts?.length ?? 0;
                  if (boundary === "completed-before-save" && savedState === "completed") crash();
                }),
              ),
            ),
        },
      }).pipe(
        Effect.provideService(KeyValueStore.KeyValueStore, {
          ...base,
          set: (recordKey, value) =>
            base.set(recordKey, value).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  if (
                    boundary === `${savedState}-saved` ||
                    (boundary === "part-checkpoint" &&
                      savedState === "uploading" &&
                      savedParts === 1)
                  )
                    crash();
                }),
              ),
            ),
        }),
      );
      const driver = {
        ...endpoint,
        storage: { ...storage, get: (request) => guard("verify", storage.get(request)) },
        multipart: {
          ...mpu,
          initiate: (request) => guard("initiate", mpu.initiate(request)),
          uploadPart: (request) =>
            mode === "abort"
              ? Effect.fail(
                  new StorageError({
                    operation: "test",
                    provider: "test",
                    reason: { _tag: "InvalidInput", field: "stop" },
                    mutationOutcome: "not-started",
                  }),
                )
              : guard(
                  "part",
                  mpu
                    .uploadPart(request)
                    .pipe(
                      Effect.tap(() =>
                        Effect.promise(() =>
                          appendFile(join(directory, "parts.log"), `${request.partNumber}\n`),
                        ),
                      ),
                    ),
                ),
          complete: (request) => guard("complete", mpu.complete(request)),
          abort: (handle) => guard("abort", mpu.abort(handle)),
        },
      };
      const managed = yield* Layer.build(
        Transfers.layer({
          partBytes: 5 * 1024 * 1024,
          partConcurrency: 1,
          maxBytes: 16 * 1024 * 1024,
        }),
      ).pipe(
        Effect.provide(
          provider.pipe(Context.add(Storage, driver.storage), Context.add(TransferDriver, driver)),
        ),
      );
      const sessions = yield* UploadSessions.make({ configurationId: "local-restart" }).pipe(
        Effect.provide(managed),
        Effect.provideService(UploadSessionStore, store),
      );
      if (mode === "download") {
        const existing = yield* sessions.status("download").pipe(Effect.result);
        if (existing._tag === "Failure") {
          const metadata = yield* storage.head({ key });
          yield* sessions.createDownload({ id: "download", ref: metadata.ref });
        }
        yield* sessions.resumeDownload("download", {
          ...target,
          write: (offset, bytes) =>
            target.write(offset, bytes).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  if (boundary === "target-write" && ++downloadWrites === 2) crash();
                }),
              ),
            ),
          publish: (checksum, length) => guard("publish", target.publish(checksum, length)),
        });
        const status = yield* sessions.status("download");
        yield* Effect.promise(() =>
          writeFile(join(directory, "result.json"), JSON.stringify(status)),
        );
        return;
      }
      const existing = yield* sessions.status("upload").pipe(Effect.result);
      if (existing._tag === "Failure") yield* sessions.create({ id: "upload", key, source });
      if (boundary === "created") crash();
      if (mode === "abort") {
        const current = yield* sessions.status("upload");
        if (current.state === "created")
          yield* sessions.resume("upload", source).pipe(Effect.result);
        yield* sessions.abort("upload").pipe(Effect.result);
      } else {
        const current = yield* sessions.status("upload");
        if (["completing", "completion-unknown"].includes(current.state))
          yield* sessions.reconcile("upload");
        yield* sessions.resume("upload", source).pipe(Effect.result);
      }
      const status = yield* sessions.status("upload");
      if (status.state === "completed") {
        yield* sessions.deliverEvents("upload", (event) =>
          Effect.promise(async () => {
            const path = join(directory, "events.json");
            let events;
            try {
              events = JSON.parse(await readFile(path, "utf8"));
            } catch {
              events = [];
            }
            if (!events.includes(event.id))
              await writeFile(path, JSON.stringify([...events, event.id]));
            if (boundary === "event-after") crash();
          }),
        );
      }
      yield* Effect.promise(() =>
        writeFile(join(directory, "result.json"), JSON.stringify(status)),
      );
    }),
  ).pipe(Effect.provide(platformLayer)),
);
