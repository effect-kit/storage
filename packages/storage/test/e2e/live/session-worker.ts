import assert from "node:assert/strict";
import { appendFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Cause, Context, Effect, Exit, Layer, Option, Redacted, Result } from "effect";
import { KeyValueStore } from "effect/persistence";

import { Storage, StorageError } from "@effect-kit/storage";
import { aes256Gcm, atomicFileStore, layer } from "@effect-kit/storage/node";
import { UploadSessions, UploadSessionStore } from "@effect-kit/storage/sessions";
import { TransferDriver, Transfers } from "@effect-kit/storage/transfers";
import type { TransferEndpoint } from "@effect-kit/storage/transfers";

import type { ProviderSettings } from "../../../../../scripts/providers/configuration.ts";
import { providerLayer } from "./adapters.ts";
import { generatedSource, partBytes } from "./fixtures.ts";
import { transferPolicy } from "./transfers.ts";

let raw = "";
for await (const chunk of process.stdin) {
  raw += chunk;
  if (Buffer.byteLength(raw) > 262144) throw new Error("Input limit");
}
const input = JSON.parse(raw) as {
  settings: ProviderSettings;
  directory: string;
  key: string;
  secret: string;
  mode: "interrupt" | "resume" | "cleanup";
};
const adapter = await providerLayer(input.settings, "portable");

const exit = await Effect.runPromiseExit(
  Effect.scoped(
    Effect.gen(function* () {
      const provider = yield* Layer.build(adapter);
      const storage = Context.get(provider, Storage);
      const endpoint = Context.get(provider, TransferDriver);
      const backing = yield* atomicFileStore(input.directory);
      const protection = aes256Gcm(Redacted.make(Buffer.from(input.secret, "base64")));
      let acknowledged = false;
      const store = yield* UploadSessionStore.make({
        namespace: "live-release",
        ownership: "exclusive",
        protection: {
          ...protection,
          seal: (plaintext, aad) =>
            protection.seal(plaintext, aad).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  const checkpoint = JSON.parse(plaintext) as {
                    state: string;
                    parts?: unknown[];
                    offset?: number;
                  };
                  acknowledged =
                    checkpoint.state === "uploading" &&
                    ((checkpoint.parts?.length ?? 0) === 1 ||
                      (checkpoint.offset ?? 0) >= partBytes);
                }),
              ),
            ),
        },
      }).pipe(
        Effect.provideService(KeyValueStore.KeyValueStore, {
          ...backing,
          set: (key, value) =>
            backing.set(key, value).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  // Exit only after the encrypted checkpoint's atomic write has completed.
                  if (input.mode === "interrupt" && acknowledged) process.exit(73);
                }),
              ),
            ),
        }),
      );
      const multipart = endpoint.multipart;
      const driver: TransferEndpoint = {
        ...endpoint,
        ...(multipart
          ? {
              multipart: {
                ...multipart,
                uploadPart: (request) =>
                  multipart
                    .uploadPart(request)
                    .pipe(
                      Effect.tap(() =>
                        Effect.promise(() =>
                          appendFile(join(input.directory, "parts.log"), `${request.partNumber}\n`),
                        ),
                      ),
                    ),
              },
            }
          : {}),
      };
      const managed = yield* Layer.build(Transfers.layer(transferPolicy)).pipe(
        Effect.provide(provider.pipe(Context.add(TransferDriver, driver))),
      );
      const sessions = yield* UploadSessions.make({ configurationId: "live-release-v1" }).pipe(
        Effect.provide(managed),
        Effect.provideService(UploadSessionStore, store),
      );

      if (input.mode === "interrupt") {
        yield* sessions.create({ id: "upload", key: input.key, source: generatedSource() });
        yield* sessions.resume("upload", generatedSource());
        throw new Error("Missing interruption boundary");
      }

      if (input.mode === "cleanup") {
        const checkpoint = yield* store.read("upload");
        if (checkpoint?.kind === "upload" && checkpoint.metadata) {
          yield* storage.delete({ ref: checkpoint.metadata.ref });
          const remaining = yield* Effect.result(storage.head({ ref: checkpoint.metadata.ref }));
          assert(Result.isFailure(remaining) && remaining.failure.reason._tag === "NotFound");
        } else if (checkpoint) yield* sessions.abort("upload");
        return;
      }

      const before = yield* sessions.status("upload");
      assert.equal(before.state, "uploading");
      const metadata = yield* sessions.resume("upload", generatedSource());
      const after = yield* sessions.status("upload");
      assert.equal(after.state, "completed");
      yield* Effect.promise(() =>
        writeFile(join(input.directory, "receipt.json"), JSON.stringify(metadata), { mode: 0o600 }),
      );
    }),
  ).pipe(Effect.provide(layer)),
);
if (Exit.isFailure(exit)) {
  const error = Option.getOrUndefined(Cause.findErrorOption(exit.cause));
  await writeFile(
    join(input.directory, "failure.json"),
    JSON.stringify(
      error instanceof StorageError
        ? {
            operation: error.operation,
            reason: error.reason._tag,
            code: error.reason._tag === "ProviderFailure" ? error.reason.code : undefined,
          }
        : { operation: "session-worker", reason: "AssertionFailed" },
    ),
    { mode: 0o600 },
  );
  process.exitCode = 1;
}
