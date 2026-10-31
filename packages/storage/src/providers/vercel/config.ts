import { Effect, Redacted, Schema } from "effect";

import { storageError } from "../../core/errors.js";
import type { StorageError } from "../../core/errors.js";

type Secret = Redacted.Redacted<string> | Effect.Effect<Redacted.Redacted<string>, StorageError>;
export interface VercelConfig {
  readonly storeId: string;
  readonly access: "public" | "private";
  readonly credential: { readonly mode: "token" | "oidc"; readonly token: Secret };
  readonly maxObjectBytes?: number;
  readonly maxUnknownLengthBytes?: number;
  readonly requestTimeoutMillis?: number;
}
const Positive = Schema.Int.check(Schema.isGreaterThan(0));
const Config = Schema.Struct({
  storeId: Schema.String.check(Schema.isPattern(/^(?:store_)?[A-Za-z0-9]+$/u)),
  access: Schema.Literals(["public", "private"]),
  credential: Schema.Struct({
    mode: Schema.Literals(["token", "oidc"]),
    token: Schema.declare<Secret>(
      (value): value is Secret => Redacted.isRedacted(value) || Effect.isEffect(value),
    ),
  }),
  maxObjectBytes: Schema.optional(Positive),
  maxUnknownLengthBytes: Schema.optional(Positive),
  requestTimeoutMillis: Schema.optional(Positive),
});
export const resolveConfig = Effect.fnUntraced(function* (input: VercelConfig) {
  yield* Schema.decodeUnknownEffect(Config, { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() =>
      storageError("make", "vercel", { _tag: "InvalidConfiguration", field: "config" }),
    ),
  );
  yield* retryPolicy();
  const override = process.env.VERCEL_BLOB_API_URL ?? process.env.NEXT_PUBLIC_VERCEL_BLOB_API_URL;
  const endpoint = override ? URL.parse(override) : undefined;
  if (
    override &&
    (!endpoint ||
      !["https:", "http:"].includes(endpoint.protocol) ||
      endpoint.username ||
      endpoint.password ||
      endpoint.search ||
      endpoint.hash ||
      endpoint.pathname !== "/")
  )
    return yield* storageError("make", "vercel", {
      _tag: "InvalidConfiguration",
      field: "VERCEL_BLOB_API_URL",
    });
  return {
    ...input,
    apiOrigin: endpoint?.origin,
    storeId: input.storeId.replace(/^store_/u, ""),
    maxObjectBytes: input.maxObjectBytes ?? 1024 ** 3,
    maxUnknownLengthBytes: input.maxUnknownLengthBytes ?? 8 * 1024 ** 2,
    requestTimeoutMillis: input.requestTimeoutMillis ?? 30000,
  };
});
export type VercelConnection = {
  readonly config: Effect.Success<ReturnType<typeof resolveConfig>>;
};
/** The pinned SDK reads this switch per request; never change global application policy here. */
export const retryPolicy = () =>
  Effect.suspend(() => {
    const version =
      process.env.VERCEL_BLOB_API_VERSION_OVERRIDE ??
      process.env.NEXT_PUBLIC_VERCEL_BLOB_API_VERSION_OVERRIDE;
    return process.env.VERCEL_BLOB_RETRIES === "0" && (version === undefined || version === "13")
      ? Effect.void
      : Effect.fail(
          storageError(
            "sdk.policy",
            "vercel",
            { _tag: "InvalidConfiguration", field: "sdk-retries-or-api-version" },
            "not-started",
          ),
        );
  });
