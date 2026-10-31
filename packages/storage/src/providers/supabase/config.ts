import { Effect, Redacted, Schema } from "effect";
import type { HttpClient } from "effect/http";

import { storageError } from "../../core/errors.js";
import type { StorageError } from "../../core/errors.js";

export interface SupabaseConfig {
  readonly url: string;
  readonly bucket: string;
  readonly storeId: string;
  readonly auth: {
    readonly mode: "user" | "service";
    readonly token:
      | Redacted.Redacted<string>
      | Effect.Effect<Redacted.Redacted<string>, StorageError>;
    readonly apiKey?: Redacted.Redacted<string>;
  };
  /** Application ceiling; configure from the project's actual global/bucket limits. */
  readonly maxObjectBytes?: number;
  readonly maxUnknownLengthBytes?: number;
  readonly requestTimeoutMillis?: number;
  /** Negotiate tus extensions once at Layer acquisition, before accepting transfers. */
  readonly resumable?: boolean;
}
const Positive = Schema.Int.check(Schema.isGreaterThan(0));
const Config = Schema.Struct({
  url: Schema.String,
  bucket: Schema.String.check(Schema.isPattern(/^(?!\.{1,2}$)[A-Za-z0-9._-]+$/u)),
  storeId: Schema.NonEmptyString,
  auth: Schema.Struct({
    mode: Schema.Literals(["user", "service"]),
    token: Schema.declare<SupabaseConfig["auth"]["token"]>(
      (value): value is SupabaseConfig["auth"]["token"] =>
        Redacted.isRedacted(value) || Effect.isEffect(value),
    ),
    apiKey: Schema.optional(Schema.Redacted(Schema.NonEmptyString)),
  }),
  maxObjectBytes: Schema.optional(Positive),
  maxUnknownLengthBytes: Schema.optional(Positive),
  requestTimeoutMillis: Schema.optional(Positive),
  resumable: Schema.optional(Schema.Boolean),
});
export const resolveConfig = Effect.fnUntraced(function* (input: SupabaseConfig) {
  yield* Schema.decodeUnknownEffect(Config, { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() =>
      storageError("make", "supabase", { _tag: "InvalidConfiguration", field: "config" }),
    ),
  );
  const url = URL.parse(input.url);
  if (
    !url ||
    !["https:", "http:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  )
    return yield* storageError("make", "supabase", { _tag: "InvalidConfiguration", field: "url" });
  return {
    ...input,
    url: url.origin,
    baseUrl: `${url.origin}/storage/v1`,
    maxObjectBytes: input.maxObjectBytes ?? 50 * 1024 ** 2,
    maxUnknownLengthBytes: input.maxUnknownLengthBytes ?? 8 * 1024 ** 2,
    requestTimeoutMillis: input.requestTimeoutMillis ?? 30000,
  };
});
export interface SupabaseConnection {
  readonly config: Effect.Success<ReturnType<typeof resolveConfig>>;
  readonly http: HttpClient.HttpClient;
}
