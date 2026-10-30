import { join } from "node:path";

import { Context, Effect, Layer, Redacted } from "effect";

import { StorageSharedKeyCredential } from "@azure/storage-blob";

import { AzureStorage } from "../../../dist/azure.js";
import { GcsStorage } from "../../../dist/gcs.js";
import * as Runtime from "../../../dist/node.js";
import { UploadSessions, UploadSessionStore } from "../../../dist/sessions.js";
import { TransferDriver, Transfers, UploadSource } from "../../../dist/transfers.js";

const [provider, endpoint, directory, command, boundary = ""] = process.argv.slice(2);
if (
  !provider ||
  !endpoint ||
  !directory ||
  !process.env.STORAGE_NATIVE_TEST_KEY ||
  !/^http:\/\/127\.0\.0\.1:\d+/u.test(endpoint)
)
  throw new Error("Owned local fixture configuration required");
const crash = (name) =>
  boundary && name === boundary
    ? Effect.promise(async () => {
        console.log(JSON.stringify({ boundary: name }));
        await new Promise(() => {});
      })
    : Effect.void;
const guard = (name, effect) => effect.pipe(Effect.tap(() => crash(name)));
const size = 600000;
const source = UploadSource.bytes(new Uint8Array(size).fill(19));
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const native =
        provider === "gcs"
          ? GcsStorage.layer({
              bucket: "test-bucket",
              apiEndpoint: endpoint,
              useAuthWithCustomEndpoint: false,
            })
          : AzureStorage.layer({
              containerUrl: endpoint,
              credential: new StorageSharedKeyCredential(
                "storagephase",
                Buffer.from("disposable-local-emulator-key-32").toString("base64"),
              ),
            });
      const context = yield* Layer.build(native);
      const driver = Context.get(context, TransferDriver);
      if (provider === "azure" && (command === "crash" || command === "example-setup"))
        yield* Effect.promise(() => Context.get(context, AzureStorage).client.createIfNotExists());
      if (command === "example-setup") return;
      const storeContext = yield* Layer.build(
        Runtime.sessionStore({
          directory: join(directory, "checkpoints"),
          namespace: "restart",
          key: Redacted.make(Buffer.from(process.env.STORAGE_NATIVE_TEST_KEY, "base64")),
          ownership: "exclusive",
        }),
      );
      const store = Context.get(storeContext, UploadSessionStore);
      const tracedStore = {
        ...store,
        write: (checkpoint, revision) =>
          store
            .write(checkpoint, revision)
            .pipe(
              Effect.tap((saved) =>
                saved.kind === "upload"
                  ? crash(
                      saved.state === "verifying"
                        ? "verifying-saved"
                        : saved.state === "uploading" && saved.protocol === "offset"
                          ? saved.offset > 0
                            ? "offset-saved"
                            : "handle-saved"
                          : saved.blocks?.length === 1 && saved.state === "uploading"
                            ? "block-saved"
                            : "",
                    )
                  : Effect.void,
              ),
            ),
      };
      const traced = {
        ...driver,
        ...(driver.offsets
          ? {
              offsets: {
                ...driver.offsets,
                write: (input) =>
                  guard(input.final ? "final-after" : "write-after", driver.offsets.write(input)),
              },
            }
          : {}),
        ...(driver.blocks
          ? {
              blocks: {
                ...driver.blocks,
                stage: (input) => guard("stage-after", driver.blocks.stage(input)),
                commit: (input) => guard("commit-after", driver.blocks.commit(input)),
              },
            }
          : {}),
      };
      const managed = yield* Layer.build(
        Transfers.layer({
          partBytes: 256 * 1024,
          maxBytes: 1024 * 1024,
          maxSourceChunkBytes: 1024 * 1024,
          memoryBudgetBytes: 8 * 1024 * 1024,
        }),
      ).pipe(Effect.provide(context.pipe(Context.add(TransferDriver, traced))));
      const sessions = yield* UploadSessions.make({ configurationId: "native-restart" }).pipe(
        Effect.provide(managed),
        Effect.provideService(UploadSessionStore, tracedStore),
      );
      if (command === "crash")
        yield* sessions.create({ id: "upload", key: directory.split("/").at(-1), source });
      const status = yield* sessions.status("upload");
      if (["completing", "completion-unknown"].includes(status.state))
        yield* sessions.reconcile("upload");
      const metadata = yield* sessions.resume("upload", source);
      console.log(
        JSON.stringify({ state: (yield* sessions.status("upload")).state, size: metadata.size }),
      );
    }),
  ).pipe(Effect.provide(Runtime.layer)),
);
