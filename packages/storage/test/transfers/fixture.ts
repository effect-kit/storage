import { Effect, Redacted } from "effect";

import type { MultipartUploads } from "../../src/contracts/multipart.js";
import type { ObjectMetadata } from "../../src/object.js";
import { MemoryStorage } from "../../src/testing.js";
import type { TransferEndpoint, TransferPolicy } from "../../src/transfers.js";
import { sha256 } from "../../src/transfers/node.js";

export const policy: TransferPolicy = {
  digest: sha256,
  memoryBudgetBytes: 512 * 1024,
  partBytes: 16,
  partConcurrency: 2,
  maxSourceChunkBytes: 256,
  maxBytes: 512,
  retryBaseMillis: 0,
  retryMaxMillis: 1,
};

export const fixture = Effect.fnUntraced(function* () {
  const storage = yield* MemoryStorage.make();
  const uploads = new Map<
    string,
    { request: Parameters<MultipartUploads["initiate"]>[0]; parts: Map<number, Uint8Array> }
  >();
  let aborts = 0;
  let initiates = 0;
  let completes = 0;
  let partAttempts = 0;
  const multipart: MultipartUploads = {
    limits: { minimumPartBytes: 4, maximumPartBytes: 64, maximumParts: 64 },
    initiate: (request) =>
      Effect.sync(() => {
        initiates++;
        const id = String(initiates);
        uploads.set(id, { request, parts: new Map() });
        return { ref: { storeId: storage.storeId, key: request.key }, uploadId: Redacted.make(id) };
      }),
    uploadPart: (request) =>
      Effect.sync(() => {
        if (!(request.body instanceof Uint8Array))
          throw new Error("fixture requires retained bytes");
        partAttempts++;
        uploads
          .get(Redacted.value(request.upload.uploadId))
          ?.parts.set(request.partNumber, request.body.slice());
        return {
          partNumber: request.partNumber,
          etag: String(request.partNumber),
          size: request.body.length,
        };
      }),
    listParts: () => Effect.succeed({ parts: [] }),
    listUploads: () => Effect.succeed({ uploads: [] }),
    complete: (request) =>
      Effect.gen(function* () {
        completes++;
        const upload = uploads.get(Redacted.value(request.upload.uploadId));
        if (!upload) throw new Error("unknown fixture upload");
        const body = Uint8Array.from(
          request.parts.flatMap((part) => [...(upload.parts.get(part.partNumber) ?? [])]),
        );
        return yield* storage.put({
          ...upload.request.headers,
          key: upload.request.key,
          body,
          condition: request.condition,
        });
      }),
    abort: () =>
      Effect.sync(() => {
        aborts++;
      }),
  };
  const endpoint: TransferEndpoint = {
    storage,
    multipart,
    maximumObjectBytes: 4096,
    maximumSingleUploadBytes: 64,
    transportOverheadBytes: 8,
    reconcile: ({ key: requestedKey, operationId, size }) =>
      storage
        .head({ key: requestedKey })
        .pipe(
          Effect.map((metadata): ObjectMetadata | undefined =>
            metadata.customMetadata?.["effect-storage-operation"] === operationId &&
            metadata.size === size
              ? metadata
              : undefined,
          ),
        ),
  };
  return {
    endpoint,
    multipart,
    counts: () => ({ aborts, initiates, completes, partAttempts }),
  };
});
