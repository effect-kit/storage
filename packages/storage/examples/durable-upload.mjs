import { Effect, Layer, Redacted } from "effect";

import * as StorageNode from "@effect-kit/storage/node";
import { S3Storage } from "@effect-kit/storage/s3";
import { UploadSessions } from "@effect-kit/storage/sessions";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";

const print = (value) => process.stdout.write(`${JSON.stringify(value)}\n`);
const [mode, id, file, key] = process.argv.slice(2);
const secret = process.env.STORAGE_CHECKPOINT_KEY;
if (!secret || !["create", "resume", "reconcile", "status", "abort"].includes(mode) || !id)
  throw new Error(
    "Supply checkpoint key and create|resume|reconcile|status|abort <unique-id> [file] [object-key]",
  );

const PlatformLive = StorageNode.layer;
const ProviderLive = S3Storage.layer({
  bucket: process.env.S3_BUCKET,
  region: process.env.S3_REGION,
  ...(process.env.S3_ENDPOINT ? { endpoint: process.env.S3_ENDPOINT, forcePathStyle: true } : {}),
});
const ManagedLive = Transfers.layer().pipe(Layer.provideMerge(ProviderLive));
const StoreLive = StorageNode.sessionStore({
  directory: process.env.STORAGE_CHECKPOINT_DIRECTORY ?? ".storage-sessions",
  namespace: "example",
  key: Redacted.make(Buffer.from(secret, "base64")),
  ownership: "exclusive",
});
const AppLive = UploadSessions.layer({
  configurationId: process.env.STORAGE_CONFIGURATION_ID ?? "example-v1",
}).pipe(
  Layer.provide(StoreLive),
  Layer.provideMerge(ManagedLive),
  Layer.provideMerge(PlatformLive),
);

await Effect.runPromise(
  Effect.gen(function* () {
    const sessions = yield* UploadSessions;
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
    const source = yield* UploadSource.file(file);
    if (mode === "create") {
      if (!key) throw new Error("Supply an object key");
      print(yield* sessions.create({ id, key, source }));
    } else print(yield* sessions.resume(id, source));
  }).pipe(Effect.provide(AppLive)),
);
