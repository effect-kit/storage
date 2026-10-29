import { randomBytes } from "node:crypto";

import { Effect, Redacted, Schema } from "effect";
import { KeyValueStore } from "effect/persistence";

import type { MultipartUploads } from "../../src/contracts/multipart.js";
import { storageError } from "../../src/errors.js";
import { aes256Gcm } from "../../src/node.js";
import { Upload } from "../../src/s3/native-models.js";
import { UploadSessions, UploadSessionStore } from "../../src/sessions.js";
import type { SessionOptions } from "../../src/sessions.js";
import type { TransferEndpoint } from "../../src/transfers.js";
import { ManagedTransferRuntime, makeManagedRuntime } from "../../src/transfers/managed-runtime.js";
import { withEndpoint, fixture, policy } from "../transfers/fixture.js";
export const handles = {
  encode: (handle: Schema.Schema.Type<typeof Upload>) =>
    Schema.encodeEffect(Schema.fromJsonString(Schema.toCodecJson(Upload)))(handle).pipe(
      Effect.map(Redacted.make),
      Effect.mapError(() => storageError("codec", "test", { _tag: "InvalidSession" })),
    ),
  decode: (encoded: Redacted.Redacted<string>) =>
    Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.toCodecJson(Upload)))(
      Redacted.value(encoded),
    ).pipe(Effect.mapError(() => storageError("codec", "test", { _tag: "InvalidSession" }))),
};
export const setup = Effect.fnUntraced(function* () {
  const test = yield* fixture();
  const parts = new Map<number, { partNumber: number; etag: string; size?: number | undefined }>();
  const endpoint: TransferEndpoint & { multipart: MultipartUploads } = {
    ...test.endpoint,
    handles,
    multipart: {
      ...test.multipart,
      uploadPart: (request) =>
        test.multipart.uploadPart(request).pipe(
          Effect.tap((part) =>
            Effect.sync(() => {
              parts.set(part.partNumber, part);
            }),
          ),
        ),
      listParts: () => Effect.succeed({ parts: [...parts.values()] }),
    },
  };
  const kvs = yield* KeyValueStore.KeyValueStore;
  const protection = aes256Gcm(Redacted.make(randomBytes(32)));
  const store = yield* UploadSessionStore.make({
    namespace: "test",
    protection,
    ownership: "exclusive",
  }).pipe(Effect.provideService(KeyValueStore.KeyValueStore, kvs));
  const make = (selected = endpoint) =>
    makeSessions(selected, store, { configurationId: "test-config" });
  return { ...test, endpoint, kvs, store, protection, make };
});

export const makeSessions = Effect.fnUntraced(function* (
  endpoint: TransferEndpoint,
  store: UploadSessionStore["Service"],
  options: SessionOptions,
  selected = policy,
) {
  const runtime = yield* withEndpoint(makeManagedRuntime(selected), endpoint);
  return yield* UploadSessions.make(options).pipe(
    Effect.provideService(ManagedTransferRuntime, runtime),
    Effect.provideService(UploadSessionStore, store),
  );
});
