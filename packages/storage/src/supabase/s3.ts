import { Context, Effect, Layer, Redacted, Schema } from "effect";

import { S3Client } from "@aws-sdk/client-s3";

import { storageError } from "../errors.js";
import { multipart } from "../s3/multipart.js";
import { primitives } from "../s3/primitives.js";
import { Storage } from "../storage.js";
import { TransferDriver } from "../transfers/dependencies.js";

export interface SupabaseS3Config {
  readonly endpoint: string;
  readonly region: string;
  readonly bucket: string;
  readonly storeId: string;
  readonly auth:
    | {
        readonly mode: "server-keys";
        readonly accessKeyId: string;
        readonly secretAccessKey: Redacted.Redacted<string>;
      }
    | {
        readonly mode: "user-session";
        readonly projectRef: string;
        readonly anonKey: Redacted.Redacted<string>;
        readonly token: () => Promise<Redacted.Redacted<string>>;
      };
  readonly maxUnknownLengthBytes?: number;
  readonly maxObjectBytes?: number;
}
const Profile = Schema.Struct({
  endpoint: Schema.String,
  region: Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/u)),
  bucket: Schema.String.check(Schema.isPattern(/^(?!\.{1,2}$)[A-Za-z0-9._-]+$/u)),
  storeId: Schema.NonEmptyString,
  auth: Schema.Union([
    Schema.Struct({
      mode: Schema.Literal("server-keys"),
      accessKeyId: Schema.NonEmptyString,
      secretAccessKey: Schema.Redacted(Schema.NonEmptyString),
    }),
    Schema.Struct({
      mode: Schema.Literal("user-session"),
      projectRef: Schema.NonEmptyString,
      anonKey: Schema.Redacted(Schema.NonEmptyString),
      token: Schema.declare<() => Promise<Redacted.Redacted<string>>>(
        (value): value is () => Promise<Redacted.Redacted<string>> => typeof value === "function",
      ),
    }),
  ]),
  maxObjectBytes: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
  maxUnknownLengthBytes: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
});
const make = Effect.fnUntraced(function* (input: SupabaseS3Config) {
  yield* Schema.decodeUnknownEffect(Profile, { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() =>
      storageError("make", "supabase-s3", { _tag: "InvalidConfiguration", field: "config" }),
    ),
  );
  const maxObjectBytes = input.maxObjectBytes ?? 50 * 1024 ** 2;

  const url = URL.parse(input.endpoint);
  if (
    !url ||
    !["http:", "https:"].includes(url.protocol) ||
    url.pathname !== "/storage/v1/s3" ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    return yield* storageError("make", "supabase-s3", {
      _tag: "InvalidConfiguration",
      field: "endpoint",
    });
  const config = {
    bucket: input.bucket,
    region: input.region,
    storeId: input.storeId,
    endpoint: input.endpoint,
    forcePathStyle: true,
    requestTimeoutMillis: 30000,
    maxUnknownLengthBytes: input.maxUnknownLengthBytes ?? 8 * 1024 ** 2,
  };
  const credentials = async () => {
    if (input.auth.mode === "server-keys")
      return {
        accessKeyId: input.auth.accessKeyId,
        secretAccessKey: Redacted.value(input.auth.secretAccessKey),
      };
    const sessionToken = Redacted.value(await input.auth.token());
    const parseExpiry = Schema.decodeUnknownSync(
      Schema.fromJsonString(Schema.Struct({ exp: Schema.Natural })),
    );
    let payload: { readonly exp: number };
    try {
      payload = parseExpiry(
        Buffer.from(sessionToken.split(".")[1] ?? "", "base64url").toString("utf8"),
      );
    } catch {
      throw storageError("credentials", "supabase-s3", { _tag: "AccessDenied" }, "not-started");
    }
    return {
      accessKeyId: input.auth.projectRef,
      secretAccessKey: Redacted.value(input.auth.anonKey),
      sessionToken,
      expiration: new Date(payload.exp * 1000),
    };
  };
  const client = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        new S3Client({
          endpoint: input.endpoint,
          region: input.region,
          credentials,
          forcePathStyle: true,
          maxAttempts: 1,
          followRegionRedirects: false,
          requestChecksumCalculation: "WHEN_REQUIRED",
          responseChecksumValidation: "WHEN_REQUIRED",
        }),
      catch: () =>
        storageError("make", "supabase-s3", { _tag: "InvalidConfiguration", field: "client" }),
    }),
    (owned) => Effect.sync(() => owned.destroy()),
  );
  const connection = {
    client,
    readChecksums: false as const,
    multipartChecksums: false as const,
    config: { ...config, endpoint: input.endpoint },
  };
  const native = multipart(connection);
  const base = primitives(connection);
  const storage = Storage.make({
    ...base,
    put: (request) =>
      base.put({
        ...request,
        maxBytes: Math.min(request.maxBytes ?? maxObjectBytes, maxObjectBytes),
      }),
    provider: "supabase-s3",
    capabilities: {
      putConditions: [],
      getConditions: ["if-match", "if-none-match"],
      deleteConditions: [],
      ranges: ["bounded", "from", "suffix"],
      versionedReads: "none",
      list: { maxPageSize: 1000, delimiter: true, cursorPersistence: "provider", snapshot: false },
    },
  });
  const uploads = {
    ...native,
    limits: { ...native.limits, completeConditions: [] },
    complete: (request: Parameters<typeof native.complete>[0]) =>
      request.condition
        ? Effect.fail(
            storageError(
              "complete",
              "supabase-s3",
              { _tag: "Unsupported", capability: "conditional-completion" },
              "not-started",
            ),
          )
        : native.complete(request),
  };
  return { ...storage, client, multipart: uploads, authMode: input.auth.mode, maxObjectBytes };
});
export type SupabaseS3Service = Effect.Success<ReturnType<typeof make>>;
export class SupabaseS3Storage extends Context.Service<SupabaseS3Storage, SupabaseS3Service>()(
  "@effect-kit/storage/supabase/SupabaseS3Storage",
) {
  static readonly make = make;
  static layer(input: SupabaseS3Config) {
    return Layer.effectContext(
      Effect.map(make(input), (storage) =>
        Context.make(Storage, storage).pipe(
          Context.add(SupabaseS3Storage, storage),
          Context.add(TransferDriver, {
            storage,
            multipart: storage.multipart,
            maximumObjectBytes: storage.maxObjectBytes,
            maximumSingleUploadBytes: Math.min(storage.maxObjectBytes, 5 * 1024 ** 3),
            transportOverheadBytes: 1024 ** 2,
          }),
        ),
      ),
    );
  }
}
