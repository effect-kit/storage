import { Effect, Redacted } from "effect";

import { UploadSessions } from "../../../dist/sessions.js";
import { Transfers, UploadSource } from "../../../dist/transfers.js";

const required = (value, field) => {
  if (!value) throw new Error(`Missing ${field}`);
  return value;
};
const [command, idOrKey, file, destinationKey] = process.argv.slice(2);
const selected = process.env.STORAGE_PROVIDER;
let application;
if (selected === "supabase") {
  const example = await import("./supabase.mjs");
  application = example.application(
    {
      url: required(process.env.STORAGE_SUPABASE_URL, "Supabase origin"),
      bucket: required(process.env.STORAGE_SUPABASE_BUCKET, "Supabase bucket"),
      storeId: required(process.env.STORAGE_STORE_ID, "namespace identity"),
      auth: {
        mode: process.env.STORAGE_SUPABASE_AUTH_MODE === "service" ? "service" : "user",
        token: Redacted.make(required(process.env.STORAGE_SUPABASE_TOKEN, "Supabase credential")),
      },
    },
    {
      directory: required(process.env.STORAGE_CHECKPOINT_DIRECTORY, "checkpoint directory"),
      namespace: "supabase-example",
      key: Redacted.make(
        Buffer.from(required(process.env.STORAGE_CHECKPOINT_KEY, "checkpoint key"), "base64"),
      ),
      ownership: "exclusive",
    },
    { configurationId: required(process.env.STORAGE_CONFIGURATION_ID, "configuration identity") },
  );
} else if (selected === "vercel") {
  const example = await import("./vercel.mjs");
  application = example.application({
    storeId: required(process.env.STORAGE_VERCEL_STORE_ID, "Vercel store"),
    access: process.env.STORAGE_VERCEL_ACCESS === "public" ? "public" : "private",
    credential: {
      mode: "token",
      token: Redacted.make(required(process.env.STORAGE_VERCEL_TOKEN, "Vercel credential")),
    },
  });
} else throw new Error("STORAGE_PROVIDER must be supabase or vercel");

const result = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const id = required(idOrKey, "key or session ID");
      if (command === "put")
        return yield* (yield* Transfers).upload({
          key: id,
          source: yield* UploadSource.file(required(file, "source file")),
        });
      if (selected !== "supabase")
        throw new Error("Durable Vercel upload is unsupported: no public multipart inventory");
      const sessions = yield* UploadSessions;
      if (command === "status") return yield* sessions.status(id);
      if (command === "reconcile") return yield* sessions.reconcile(id);
      const source = yield* UploadSource.file(required(file, "source file"));
      if (command === "create")
        return {
          id: yield* sessions.create({
            id,
            key: required(destinationKey, "destination key"),
            source,
          }),
        };
      if (command === "resume") return yield* sessions.resume(id, source);
      throw new Error("Use put, create, resume, status or reconcile");
    }),
  ).pipe(Effect.provide(application)),
);
process.stdout.write(`${JSON.stringify(result)}\n`);
