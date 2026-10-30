import { randomBytes } from "node:crypto";

import { Effect, Redacted } from "effect";
import { KeyValueStore } from "effect/persistence";

import { aes256Gcm } from "../../src/runtime/node-protection.js";
import { Storage } from "../../src/storage.js";
import { MemoryStorage } from "../../src/testing.js";
import { Uploads, UploadStore, UploadGrantDriver } from "../../src/uploads.js";
import type { UploadOptions } from "../../src/uploads.js";
import type { UploadGrantDriverService } from "../../src/uploads/driver.js";
import { digestText } from "../../src/uploads/verification.js";
export const actor = { subject: "user-a", tenantId: "tenant-a" };
export const input = {
  purpose: "attachment",
  context: { documentId: "owned-document" },
  file: { name: "file.txt", size: 5, contentType: "text/plain", sha256: digestText("hello") },
  strategy: "proxy" as const,
};
export const policy: UploadOptions = {
  quarantine: "private",
  authorize: ({ actor: current, uploadId }) =>
    Effect.succeed({
      key: `private/${current.tenantId}/${uploadId}`,
      context: { documentId: "owned-document" },
      maxBytes: 1024 * 1024,
      allowedContentTypes: ["text/plain"],
      expiresInSeconds: 900,
    }),
  publish: ({ object }) => Effect.succeed({ fileId: object.ref.key }),
};
export const setup = Effect.fnUntraced(function* (
  options: Partial<UploadOptions> = {},
  supplied?: UploadGrantDriverService,
) {
  const storage = yield* MemoryStorage.make({ storeId: "owned" });
  const kvs = yield* KeyValueStore.KeyValueStore;
  const protection = aes256Gcm(Redacted.make(randomBytes(32)));
  const makeStore = () =>
    UploadStore.make({ namespace: "uploads", ownership: "exclusive", protection });
  const store = yield* makeStore();
  const driver: UploadGrantDriverService = supplied ?? {
    begin: () => Effect.succeed(null),
    grant: (_record, expiresAt) => Effect.succeed({ _tag: "Proxy", expiresAt }),
    abort: () => Effect.void,
  };
  const make = (selected = store) =>
    Uploads.make({ ...policy, ...options }).pipe(
      Effect.provideService(Storage, storage),
      Effect.provideService(UploadStore, selected),
      Effect.provideService(UploadGrantDriver, driver),
    );
  return { storage, kvs, store, makeStore, make, uploads: yield* make() };
});
