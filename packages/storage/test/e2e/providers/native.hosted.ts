/* oxlint-disable no-await-in-loop -- Cleanup enumerates only this run's unique prefix and removes observed versions serially. */
import { randomUUID } from "node:crypto";

import { assert, it } from "@effect/vitest";
import { Context, Effect, Layer, Schema } from "effect";

import { DefaultAzureCredential } from "@azure/identity";

import { storageError } from "../../../src/core/errors.js";
import { Storage } from "../../../src/core/storage.js";
import { AzureStorage } from "../../../src/providers/azure/index.js";
import { GcsStorage } from "../../../src/providers/gcs/index.js";
import { metadata, NativeMetadata } from "../../../src/providers/gcs/mapping.js";
import { decodeJson, jsonRequest } from "../../../src/providers/gcs/transport.js";
import * as Runtime from "../../../src/runtime/node.js";
import { TransferDriver, Transfers, UploadSource } from "../../../src/transfers/index.js";

const environment = Schema.decodeUnknownSync(
  Schema.Struct({
    provider: Schema.Literals(["gcs", "azure"]),
    prefix: Schema.String.check(Schema.isPattern(/^effect-storage-acceptance\/[A-Za-z0-9_-]+\/$/u)),
  }),
)({
  provider: process.env.STORAGE_NATIVE_HOSTED_PROVIDER,
  prefix: process.env.STORAGE_NATIVE_TEST_PREFIX,
});
const prefix = `${environment.prefix}${randomUUID()}/`;
const azureUrl = process.env.STORAGE_NATIVE_AZURE_CONTAINER_URL;
if (
  environment.provider === "azure" &&
  (!azureUrl || !/^https:\/\/[a-z0-9]+\.blob\.core\.windows\.net\/[a-z0-9-]+$/u.test(azureUrl))
)
  throw new Error("An existing Azure public-cloud test container is required");
const gcsBucket = process.env.STORAGE_NATIVE_GCS_BUCKET;
if (environment.provider === "gcs" && !gcsBucket)
  throw new Error("An existing GCS test bucket is required");
const fixture = Effect.gen(function* () {
  const selected = yield* Effect.gen(function* () {
    if (environment.provider === "gcs") {
      const context = yield* Layer.build(GcsStorage.layer({ bucket: gcsBucket ?? "invalid" }));
      return {
        context: context.pipe(Context.pick(Storage, TransferDriver)),
        native: { kind: "gcs" as const, service: Context.get(context, GcsStorage) },
      };
    }
    const context = yield* Layer.build(
      AzureStorage.layer({
        containerUrl: azureUrl ?? "https://invalid.blob.core.windows.net/test",
        credential: new DefaultAzureCredential(),
      }),
    );
    return {
      context: context.pipe(Context.pick(Storage, TransferDriver)),
      native: { kind: "azure" as const, service: Context.get(context, AzureStorage) },
    };
  });
  const { context, native: selectedNative } = selected;
  const storage = Context.get(context, Storage);
  yield* Effect.addFinalizer(() =>
    selectedNative.kind === "gcs"
      ? Effect.gen(function* () {
          const native = selectedNative.service;
          let token: string | undefined;
          do {
            const response = yield* jsonRequest(native, "fixture-cleanup", false, {
              uri: `${native.config.apiEndpoint}/storage/v1/b/${encodeURIComponent(native.config.bucket)}/o`,
              qs: { prefix, versions: "true", maxResults: 100, pageToken: token },
              json: true,
            });
            const page = yield* decodeJson(
              Schema.Struct({
                items: Schema.optional(Schema.Array(NativeMetadata)),
                nextPageToken: Schema.optional(Schema.String),
              }),
              response.body,
              "fixture-cleanup",
            );
            for (const entry of page.items ?? []) {
              if (!entry.name.startsWith(prefix))
                return yield* Effect.die(new Error("Cleanup escaped owned prefix"));
              const observed = yield* metadata(native, entry, "fixture-cleanup");
              yield* native.delete({ ref: observed.ref });
            }
            token = page.nextPageToken;
          } while (token);
        }).pipe(Effect.orDie)
      : Effect.promise(async () => {
          const native = selectedNative.service;
          for await (const blob of native.client.listBlobsFlat({
            prefix,
            includeVersions: true,
            includeSnapshots: true,
          })) {
            if (!blob.name.startsWith(prefix)) throw new Error("Cleanup escaped owned prefix");
            let client = native.client.getBlobClient(blob.name);
            if (blob.versionId) client = client.withVersion(blob.versionId);
            else if (blob.snapshot) client = client.withSnapshot(blob.snapshot);
            await client.delete();
          }
        }),
  );
  return { context, storage, native: selectedNative };
});

it.live(
  "hosted native generation/ETag conditions and exact identity survive competing writes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { storage, native: selectedNative } = yield* fixture;
        const key = `${prefix}conditions`;
        const initial = yield* storage.put({
          key,
          body: new Uint8Array([1]),
          condition: { _tag: "CreateOnly" },
        });
        if (selectedNative.kind === "gcs") {
          const native = selectedNative.service;
          const generation = initial.ref.version?.id;
          if (!generation) return yield* storageError("test", "gcs", { _tag: "ProviderFailure" });
          const results = yield* Effect.all(
            [2, 3].map((byte) =>
              native
                .putObject({ key, body: new Uint8Array([byte]) }, { ifGenerationMatch: generation })
                .pipe(Effect.result),
            ),
            { concurrency: 2 },
          );
          assert.strictEqual(results.filter((result) => result._tag === "Success").length, 1);
          assert.strictEqual(
            results.filter(
              (result) =>
                result._tag === "Failure" && result.failure.reason._tag === "PreconditionFailed",
            ).length,
            1,
          );
          const observed = yield* native.head({ key });
          const updated = yield* native.updateMetadata({
            ref: observed.ref,
            headers: { contentType: "application/octet-stream" },
            preconditions: {
              ifMetagenerationMatch:
                observed.ref.version?._tag === "GcsGeneration"
                  ? (observed.ref.version.metageneration ?? "invalid")
                  : "invalid",
            },
          });
          assert.strictEqual(updated.ref.version?.id, observed.ref.version?.id);
        } else {
          if (!initial.etag)
            return yield* storageError("test", "azure", { _tag: "ProviderFailure" });
          const etag = initial.etag;
          const results = yield* Effect.all(
            [2, 3].map((byte) =>
              storage
                .put({ key, body: new Uint8Array([byte]), condition: { _tag: "IfMatch", etag } })
                .pipe(Effect.result),
            ),
            { concurrency: 2 },
          );
          assert.strictEqual(results.filter((result) => result._tag === "Success").length, 1);
          const native = selectedNative.service;
          const ref = { storeId: storage.storeId, key };
          const lease = yield* native.acquireLease({ ref, durationSeconds: 15 });
          yield* Effect.addFinalizer(() =>
            native.releaseLease({ ref, leaseId: lease }).pipe(Effect.orDie),
          );
          const denied = yield* storage.put({ key, body: new Uint8Array([4]) }).pipe(Effect.result);
          assert.strictEqual(
            denied._tag === "Failure" && denied.failure.reason._tag,
            "PreconditionFailed",
          );
        }
      }),
    ),
);

it.live("hosted native transfer verifies the exact published object", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { context } = yield* fixture;
      const transfersContext = yield* Layer.build(
        Transfers.layer({ partBytes: 256 * 1024, maxBytes: 1024 * 1024 }),
      ).pipe(Effect.provide(context));
      const transfers = Context.get(transfersContext, Transfers);
      const receipt = yield* transfers.upload({
        key: `${prefix}transfer`,
        source: UploadSource.bytes(new Uint8Array(600000).fill(19)),
        strategy: environment.provider === "gcs" ? "offset" : "blocks",
      });
      assert.strictEqual(receipt.metadata.size, 600000);
      assert.strictEqual(receipt.progress.phase, "verified");
    }),
  ).pipe(Effect.provide(Runtime.layer)),
);
