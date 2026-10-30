import { Effect, Redacted } from "effect";

import { Storage } from "../dist/index.js";
import { UploadSessions } from "../dist/sessions.js";
import { UploadSource } from "../dist/transfers.js";

const required = (value, field) => {
  if (!value) throw new Error(`Missing ${field}`);
  return value;
};
const [command, idOrKey, file, destinationKey] = process.argv.slice(2);
const provider = process.env.STORAGE_PROVIDER;
const persistence = {
  directory: required(process.env.STORAGE_CHECKPOINT_DIRECTORY, "checkpoint directory"),
  namespace: "native-example",
  key: Redacted.make(
    Buffer.from(required(process.env.STORAGE_CHECKPOINT_KEY, "checkpoint key"), "base64"),
  ),
  ownership: "exclusive",
};
const options = {
  configurationId: required(process.env.STORAGE_CONFIGURATION_ID, "configuration identity"),
};
let application;
if (provider === "gcs") {
  const example = await import("./gcs.mjs");
  application = example.application(
    {
      bucket: required(process.env.GCS_BUCKET, "GCS bucket"),
      ...(process.env.GCS_PROJECT_ID ? { projectId: process.env.GCS_PROJECT_ID } : {}),
      ...(process.env.GCS_ENDPOINT
        ? {
            apiEndpoint: process.env.GCS_ENDPOINT,
            useAuthWithCustomEndpoint: process.env.GCS_ANONYMOUS !== "true",
          }
        : {}),
    },
    persistence,
    options,
  );
} else if (provider === "azure") {
  const example = await import("./azure.mjs");
  const url = required(process.env.AZURE_STORAGE_CONTAINER_URL, "Azure container URL");
  const parsed = URL.parse(url);
  if (!parsed) throw new Error("Invalid Azure container URL");
  let credential;
  if (process.env.AZURE_STORAGE_ACCOUNT && process.env.AZURE_STORAGE_ACCESS_KEY) {
    const { StorageSharedKeyCredential } = await import("@azure/storage-blob");
    credential = new StorageSharedKeyCredential(
      process.env.AZURE_STORAGE_ACCOUNT,
      process.env.AZURE_STORAGE_ACCESS_KEY,
    );
  } else if (!parsed.search) {
    const { defaultCredential } = await import("../dist/azure/identity.js");
    credential = defaultCredential();
  }
  application = example.application(
    { containerUrl: Redacted.make(url), ...(credential ? { credential } : {}) },
    persistence,
    options,
  );
} else throw new Error("STORAGE_PROVIDER must be gcs or azure");

const result = await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const id = required(idOrKey, "object key or session ID");
      const sessions = yield* UploadSessions;
      if (command === "status") return yield* sessions.status(id);
      if (command === "reconcile") return yield* sessions.reconcile(id);
      const source = yield* UploadSource.file(required(file, "source file"));
      if (command === "put") {
        const storage = yield* Storage;
        return yield* storage.put({ key: id, body: source.open(), contentLength: source.length });
      }
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
