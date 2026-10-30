import { Clock, DateTime, Effect, Layer, Redacted, Schema } from "effect";

import { AzureStorage } from "../azure.js";
import { StorageSigning } from "../capabilities.js";
import { UploadGrantDriver } from "../uploads/driver.js";
import { failUpload } from "../uploads/schema.js";
import { operationMetadata } from "../uploads/verification.js";
const blockId = (number: number) =>
  Buffer.from(`effect-upload-${String(number).padStart(5, "0")}`).toString("base64");
const remote = (input: unknown) =>
  Schema.decodeUnknownEffect(Schema.Struct({ partBytes: Schema.Natural }))(input).pipe(
    Effect.mapError(() => failUpload("Conflict")),
  );

export const AzureUploadGrants = {
  layer(options: { readonly partBytes?: number } = {}) {
    return Layer.effect(
      UploadGrantDriver,
      Effect.gen(function* () {
        const storage = yield* AzureStorage;
        const signing = yield* StorageSigning;
        const partBytes = options.partBytes ?? 8 * 1024 ** 2;
        if (!Number.isSafeInteger(partBytes) || partBytes < 1024 ** 2 || partBytes > 64 * 1024 ** 2)
          return yield* failUpload("BadRequest");
        return UploadGrantDriver.of({
          begin: (record) =>
            ["auto", "multipart"].includes(record.input.strategy ?? "auto") &&
            Math.ceil(record.input.file.size / partBytes) <= 10000 &&
            record.input.file.size > 0
              ? Effect.succeed({ partBytes })
              : Effect.fail(failUpload("Unsupported")),
          grant: Effect.fnUntraced(function* (record, expiresAt) {
            const state = yield* remote(record.remote ? Redacted.value(record.remote) : undefined);
            return { _tag: "Blocks", partBytes: state.partBytes, completed: [], expiresAt };
          }),
          part: Effect.fnUntraced(function* (record, partNumber, expiresAt) {
            const state = yield* remote(record.remote ? Redacted.value(record.remote) : undefined);
            if (partNumber > Math.ceil(record.input.file.size / state.partBytes))
              return yield* failUpload("BadRequest");
            const signed = yield* signing.sign({
              ref: { storeId: storage.storeId, key: record.stagingKey },
              method: "PUT",
              expiresInSeconds: Math.max(
                1,
                Math.floor((expiresAt - (yield* Clock.currentTimeMillis)) / 1000),
              ),
            });
            const url = new URL(Redacted.value(signed.url));
            url.searchParams.set("comp", "block");
            url.searchParams.set("blockid", blockId(partNumber));
            return {
              _tag: "SignedRequest",
              method: "PUT",
              url: url.href,
              headers: Object.fromEntries(
                Object.entries(Redacted.value(signed.headers)).filter(
                  ([name]) => name !== "x-ms-blob-type",
                ),
              ),
              partReceipt: blockId(partNumber),
              expiresAt: DateTime.toEpochMillis(signed.expiresAt),
            };
          }),
          complete: Effect.fnUntraced(function* (record, supplied) {
            const state = yield* remote(record.remote ? Redacted.value(record.remote) : undefined);
            const count = Math.ceil(record.input.file.size / state.partBytes);
            const inventory = yield* storage.blocks.list({
              storeId: storage.storeId,
              key: record.stagingKey,
            });
            if (
              supplied.length !== count ||
              new Set(supplied.map((part) => part.partNumber)).size !== count
            )
              return yield* failUpload("VerificationFailed");
            const ids: string[] = [];
            for (let number = 1; number <= count; number++) {
              const id = blockId(number);
              if (
                !supplied.some((part) => part.partNumber === number && part.receipt === id) ||
                !inventory.uncommitted.some(
                  (part) =>
                    part.blockId === id &&
                    part.size ===
                      Math.min(
                        state.partBytes,
                        record.input.file.size - (number - 1) * state.partBytes,
                      ),
                )
              )
                return yield* failUpload("VerificationFailed");
              ids.push(id);
            }
            yield* storage.blocks.commit({
              ref: { storeId: storage.storeId, key: record.stagingKey },
              blockIds: ids,
              headers: {
                contentType: record.input.file.contentType,
                customMetadata: operationMetadata(record),
              },
              condition: { _tag: "CreateOnly" },
            });
          }),
          // Azure has no operation that abandons only these uncommitted blocks. Native expiry owns them.
          abort: () => Effect.void,
        });
      }),
    );
  },
};
