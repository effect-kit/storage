import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";

import { Effect, Redacted, Stream } from "effect";
import { KeyValueStore } from "effect/persistence";

import { S3Storage } from "@effect-kit/storage/s3";
import { s3SessionHandles, s3TransferEndpoint } from "@effect-kit/storage/s3/transfers";
import { UploadSessions, UploadSessionStore } from "@effect-kit/storage/sessions";
import { atomicFileStore, aes256Gcm } from "@effect-kit/storage/sessions/node";
import { UploadSource } from "@effect-kit/storage/transfers";
import { sha256 } from "@effect-kit/storage/transfers/node";

const print = (value) => process.stdout.write(`${JSON.stringify(value)}\n`);

// Supply a stable secret from your application's secret configuration, not the checkpoint directory.
const [mode, id, file, key] = process.argv.slice(2);
const secret = process.env.STORAGE_CHECKPOINT_KEY;
if (!secret || !["create", "resume", "reconcile", "status", "abort"].includes(mode) || !id)
  throw new Error(
    "Supply checkpoint key and create|resume|reconcile|status|abort <unique-id> [file] [object-key]",
  );
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* S3Storage.make({
        bucket: process.env.S3_BUCKET,
        region: process.env.S3_REGION,
        ...(process.env.S3_ENDPOINT
          ? { endpoint: process.env.S3_ENDPOINT, forcePathStyle: true }
          : {}),
      });
      const kvs = yield* atomicFileStore(
        process.env.STORAGE_CHECKPOINT_DIRECTORY ?? ".storage-sessions",
      );
      const store = yield* UploadSessionStore.make({
        namespace: "example",
        ownership: "exclusive",
        protection: aes256Gcm(Redacted.make(Buffer.from(secret, "base64"))),
      }).pipe(Effect.provideService(KeyValueStore.KeyValueStore, kvs));
      const sessions = yield* UploadSessions.make(
        s3TransferEndpoint(storage),
        { digest: sha256 },
        {
          configurationId: process.env.STORAGE_CONFIGURATION_ID ?? "example-v1",
          handles: s3SessionHandles,
        },
      ).pipe(Effect.provideService(UploadSessionStore, store));
      if (mode === "status") {
        print(yield* sessions.status(id));
        return;
      }
      if (mode === "reconcile") {
        print(yield* sessions.reconcile(id));
        return;
      }
      if (mode === "abort") {
        yield* sessions.abort(id);
        return;
      }
      if (!file) throw new Error("Supply a stable file for create/resume");
      const info = yield* Effect.promise(() => stat(file));
      const source = UploadSource.replayable(
        () =>
          Stream.fromAsyncIterable(
            createReadStream(file, { highWaterMark: 65536 }),
            () => new Error("Source read failed"),
          ),
        info.size,
      );
      if (mode === "create") {
        if (!key) throw new Error("Supply an object key");
        print(yield* sessions.create({ id, key, source }));
      } else print(yield* sessions.resume(id, source));
    }),
  ),
);
