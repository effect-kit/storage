import { Effect, Layer } from "effect";

import * as StorageBun from "../dist/bun.js";
import { FilesystemStorage } from "../dist/filesystem.js";
import { Storage } from "../dist/index.js";

const root = process.env.STORAGE_OBJECT_DIRECTORY;
if (!root)
  throw new Error("Set STORAGE_OBJECT_DIRECTORY to an application-owned private directory");
const AppLive = FilesystemStorage.layer({ root, storeId: "filesystem-bun-example" }).pipe(
  Layer.provide(StorageBun.layer),
);
const program = Effect.gen(function* () {
  const storage = yield* Storage;
  yield* storage.put({
    key: "example/hello.txt",
    body: new TextEncoder().encode("hello"),
    contentType: "text/plain",
  });
  return yield* Storage.getText({ key: "example/hello.txt", maxBytes: 1024 });
});
process.stdout.write(`${await Effect.runPromise(program.pipe(Effect.provide(AppLive)))}\n`);
