import { Clock, Effect, Layer, Redacted, Schema } from "effect";
import { Base64Url } from "effect/encoding";

import { UploadGrantDriver } from "../../uploads/driver.js";
import { failUpload } from "../../uploads/schema.js";
import type { UploadActor } from "../../uploads/schema.js";
import { operationMetadata } from "../../uploads/verification.js";
import { SupabaseStorage } from "./index.js";
import { negotiateTus, tusUploads } from "./tus.js";

const decode = (value: unknown) =>
  Schema.decodeUnknownEffect(
    Schema.Struct({ url: Schema.String, total: Schema.Natural, expiresAt: Schema.Number }),
  )(value).pipe(Effect.mapError(() => failUpload("Conflict")));

export const SupabaseTusUploadGrants = {
  layer<R>(options: {
    readonly userToken: (
      actor: UploadActor,
    ) => Effect.Effect<Redacted.Redacted<string>, ReturnType<typeof failUpload>, R>;
    readonly publicApiKey: Redacted.Redacted<string>;
  }) {
    return Layer.effect(
      UploadGrantDriver,
      Effect.gen(function* () {
        const storage = yield* SupabaseStorage;
        const context = yield* Effect.context<R>();
        if (!storage.offsets) return yield* failUpload("Unsupported");
        const extensions = yield* negotiateTus(storage);
        const credentials = Effect.fnUntraced(function* (actor: UploadActor) {
          const token = yield* Effect.provide(options.userToken(actor), context);
          const payload = yield* Effect.fromResult(
            Base64Url.decodeString(Redacted.value(token).split(".")[1] ?? ""),
          ).pipe(Effect.mapError(() => failUpload("Unauthorized")));
          const claims = yield* Schema.decodeUnknownEffect(
            Schema.fromJsonString(
              Schema.Struct({
                sub: Schema.String,
                role: Schema.Literal("authenticated"),
                exp: Schema.Natural,
              }),
            ),
          )(payload).pipe(Effect.mapError(() => failUpload("Unauthorized")));
          if (claims.sub !== actor.subject || claims.exp * 1000 <= (yield* Clock.currentTimeMillis))
            return yield* failUpload("Unauthorized");
          const native = tusUploads(
            {
              ...storage,
              config: {
                ...storage.config,
                auth: { mode: "user" as const, token, apiKey: options.publicApiKey },
              },
            },
            extensions,
          );
          return { token, native, expiresAt: claims.exp * 1000 };
        });
        return UploadGrantDriver.of({
          maximumGrantLifetimeMillis: 86400000,
          begin: Effect.fnUntraced(function* (record) {
            if (!["auto", "resumable"].includes(record.input.strategy ?? "auto"))
              return yield* failUpload("Unsupported");
            const user = yield* credentials(record.actor);
            const upload = yield* user.native.create({
              key: record.stagingKey,
              contentLength: record.input.file.size,
              contentType: record.input.file.contentType,
              customMetadata: operationMetadata(record),
              condition: { _tag: "CreateOnly" },
            });
            const handle = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(Schema.Struct({ url: Schema.String, total: Schema.Natural })),
            )(Redacted.value(upload.handle)).pipe(Effect.mapError(() => failUpload("Unavailable")));
            return { ...handle, expiresAt: (yield* Clock.currentTimeMillis) + 1 * 86400000 };
          }),
          grant: Effect.fnUntraced(function* (record, expiresAt) {
            const user = yield* credentials(record.actor);
            const remote = yield* decode(record.remote ? Redacted.value(record.remote) : undefined);
            const state = yield* user.native.query({
              protocol: "supabase-tus-v1",
              ref: { storeId: storage.storeId, key: record.stagingKey },
              handle: Redacted.make(JSON.stringify(remote)),
            });
            if (state._tag === "Expired") return yield* failUpload("Expired");
            if (state._tag === "Unknown") return yield* failUpload("Unavailable");
            return {
              _tag: "Resumable",
              protocol: "tus",
              url: remote.url,
              headers: {
                authorization: `Bearer ${Redacted.value(user.token)}`,
                apikey: Redacted.value(options.publicApiKey),
              },
              offset: state._tag === "Completed" ? record.input.file.size : state.committedOffset,
              chunkBytes: 6 * 1024 ** 2,
              expiresAt: Math.min(expiresAt, user.expiresAt, remote.expiresAt),
            };
          }),
          abort: Effect.fnUntraced(function* (record) {
            if (record.remote === undefined) return;
            const user = yield* credentials(record.actor);
            const remote = yield* decode(record.remote ? Redacted.value(record.remote) : undefined);
            yield* user.native.cancel({
              protocol: "supabase-tus-v1",
              ref: { storeId: storage.storeId, key: record.stagingKey },
              handle: Redacted.make(JSON.stringify(remote)),
            });
          }),
        });
      }),
    );
  },
};
