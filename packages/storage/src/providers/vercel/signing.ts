import { Clock, Context, DateTime, Effect, Layer, Redacted, Schema } from "effect";

import { issueSignedToken, parseStoreIdFromDelegationToken, presignUrl } from "@vercel/blob";
import type { DelegationOperation } from "@vercel/blob";

import { StorageSigning } from "../../core/capabilities.js";
import type { SignedRequest } from "../../core/capabilities.js";
import { storageError } from "../../core/errors.js";
import type { ObjectRef } from "../../core/object.js";
import { ObjectKey } from "../../core/object.js";
import { signingRequest } from "../../internal/signing-request.js";
import { VercelStorage } from "./index.js";
import { validateKey } from "./mapping.js";
import { call } from "./transport.js";

export class VercelUploadGrants extends Context.Service<
  VercelUploadGrants,
  {
    readonly create: (input: {
      readonly key: string;
      readonly expiresInSeconds: number;
      readonly maximumSizeInBytes: number;
      readonly allowedContentTypes?: ReadonlyArray<string>;
      readonly allowOverwrite?: boolean;
    }) => Effect.Effect<SignedRequest, ReturnType<typeof storageError>>;
  }
>()("@effect-kit/storage/vercel/VercelUploadGrants") {}

export const VercelSigning = {
  layer() {
    return Layer.effectContext(
      Effect.gen(function* () {
        const storage = yield* VercelStorage;
        const issue = Effect.fnUntraced(function* (
          ref: ObjectRef,
          method: SignedRequest["method"],
          expiresInSeconds: number,
          upload?: {
            maximumSizeInBytes: number;
            allowedContentTypes?: ReadonlyArray<string>;
            allowOverwrite?: boolean;
          },
        ) {
          yield* signingRequest({ ref, method, expiresInSeconds }, "vercel");
          yield* validateKey(ref.key, "sign");
          if (ref.storeId !== storage.storeId || ref.version || /[*?#%\\]/u.test(ref.key))
            return yield* storageError(
              "sign",
              "vercel",
              { _tag: "InvalidInput", field: "ref" },
              "not-started",
            );
          const now = yield* Clock.currentTimeMillis;
          const requestedExpiry = now + expiresInSeconds * 1000;
          const operation = method.toLowerCase() as DelegationOperation;
          const signed = yield* call(storage, "sign", false, (options) =>
            issueSignedToken({
              ...options,
              pathname: ref.key,
              operations: [operation],
              validUntil: requestedExpiry,
              ...(upload
                ? {
                    maximumSizeInBytes: upload.maximumSizeInBytes,
                    ...(upload.allowedContentTypes
                      ? { allowedContentTypes: [...upload.allowedContentTypes] }
                      : {}),
                  }
                : {}),
            }),
          );
          yield* Schema.decodeUnknownEffect(
            Schema.Struct({
              validUntil: Schema.Natural,
              delegationToken: Schema.NonEmptyString,
              clientSigningToken: Schema.NonEmptyString,
            }),
          )(signed).pipe(
            Effect.mapError(() => storageError("sign", "vercel", { _tag: "ProviderFailure" })),
          );
          const signedStoreId = yield* Effect.try({
            try: () => parseStoreIdFromDelegationToken(signed.delegationToken),
            catch: () => storageError("sign", "vercel", { _tag: "ProviderFailure" }),
          });
          if (signedStoreId !== storage.config.storeId)
            return yield* storageError("sign", "vercel", { _tag: "ProviderFailure" });
          const expiry = Math.min(requestedExpiry, signed.validUntil);
          if (expiry <= (yield* Clock.currentTimeMillis))
            return yield* storageError("sign", "vercel", { _tag: "Expired" });
          const result = yield* Effect.tryPromise({
            try: () =>
              presignUrl(signed, {
                operation,
                pathname: ref.key,
                access: storage.config.access,
                validUntil: expiry,
                ...(operation === "put"
                  ? {
                      addRandomSuffix: false,
                      allowOverwrite: upload?.allowOverwrite ?? false,
                      ...(upload
                        ? {
                            maximumSizeInBytes: upload.maximumSizeInBytes,
                            ...(upload.allowedContentTypes
                              ? { allowedContentTypes: [...upload.allowedContentTypes] }
                              : {}),
                          }
                        : {}),
                    }
                  : {}),
              }),
            catch: () => storageError("sign", "vercel", { _tag: "ProviderFailure" }),
          });
          return {
            method,
            ref,
            url: Redacted.make(result.presignedUrl),
            headers: Redacted.make({}),
            expiresAt: DateTime.makeUnsafe(expiry),
            constraints: upload
              ? [
                  {
                    kind: "maximum-size",
                    value: upload.maximumSizeInBytes,
                    enforcement: "provider" as const,
                  },
                  {
                    kind: "overwrite",
                    value: String(upload.allowOverwrite ?? false),
                    enforcement: "provider" as const,
                  },
                  ...(upload.allowedContentTypes
                    ? [
                        {
                          kind: "content-types",
                          value: upload.allowedContentTypes.join(","),
                          enforcement: "provider" as const,
                        },
                      ]
                    : []),
                ]
              : [],
          } satisfies SignedRequest;
        });
        const create: VercelUploadGrants["Service"]["create"] = Effect.fnUntraced(
          function* (input) {
            yield* Schema.decodeUnknownEffect(
              Schema.Struct({
                key: ObjectKey,
                expiresInSeconds: Schema.Int.check(
                  Schema.isBetween({ minimum: 1, maximum: 604800 }),
                ),
                maximumSizeInBytes: Schema.Int.check(Schema.isGreaterThan(0)),
                allowedContentTypes: Schema.optional(
                  Schema.Array(Schema.NonEmptyString).check(Schema.isMinLength(1)),
                ),
                allowOverwrite: Schema.optional(Schema.Boolean),
              }),
            )(input).pipe(
              Effect.mapError(() =>
                storageError(
                  "grant",
                  "vercel",
                  { _tag: "InvalidInput", field: "request" },
                  "not-started",
                ),
              ),
            );
            if (input.maximumSizeInBytes > storage.config.maxObjectBytes)
              return yield* storageError(
                "grant",
                "vercel",
                { _tag: "LimitExceeded", limit: storage.config.maxObjectBytes },
                "not-started",
              );
            return yield* issue(
              { storeId: storage.storeId, key: input.key },
              "PUT",
              input.expiresInSeconds,
              input,
            );
          },
        );
        return Context.make(StorageSigning, {
          sign: (input) => issue(input.ref, input.method, input.expiresInSeconds),
        }).pipe(Context.add(VercelUploadGrants, { create }));
      }),
    );
  },
};
