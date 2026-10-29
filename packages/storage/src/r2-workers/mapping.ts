import { Effect, Option, Schema } from "effect";
import { Hex } from "effect/encoding";

import { storageError } from "../errors.js";
import type { MutationOutcome, StorageReason } from "../errors.js";
import { ObjectMetadata } from "../object.js";
import type { ObjectHeaders } from "../object.js";
import type { R2ObjectLike, R2PutOptions } from "./binding.js";

const NativeError = Schema.Struct({
  status: Schema.optional(Schema.Number),
  httpStatus: Schema.optional(Schema.Number),
  message: Schema.optional(Schema.String),
  code: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
});
const invalidCodes = new Set([
  "10011",
  "10012",
  "10020",
  "10022",
  "10025",
  "10029",
  "10048",
  "100100",
  "10013",
  "10014",
  "10033",
]);
const nativeReason = (status: number | undefined, code: string | undefined): StorageReason => {
  if (status === 403 || ["10002", "10003", "10035", "10042", "10069"].includes(code ?? ""))
    return { _tag: "AccessDenied" };
  if (status === 404 || code === "10007" || code === "10024") return { _tag: "NotFound" };
  if (status === 412 || code === "10031") return { _tag: "PreconditionFailed" };
  if (status === 416 || code === "10039") return { _tag: "RangeNotSatisfiable" };
  if (status === 429 || code === "10058") return { _tag: "Throttled" };
  if (code === "10018") return { _tag: "Expired" };
  if (code === "10054") return { _tag: "TransportFailure", code: "ClientDisconnect" };
  if (code === "10037") return { _tag: "IntegrityMismatch" };
  if (status === 503 || code === "10043")
    return { _tag: "ProviderFailure", code: "ServiceUnavailable" };
  if (code === "10001") return { _tag: "ProviderFailure", code: "InternalError" };
  if (invalidCodes.has(code ?? "")) return { _tag: "InvalidInput", field: "native-request" };
  return { _tag: "ProviderFailure", code: "R2BindingFailure" };
};

export const bindingError = (operation: string, outcome: MutationOutcome) => (error: unknown) => {
  const parsed = Schema.decodeUnknownOption(NativeError)(error);
  const details = Option.isSome(parsed) ? parsed.value : undefined;
  const status = details?.httpStatus ?? details?.status;
  // workerd exposes numeric R2 codes in Error messages. Only recognized codes become public reasons.
  const code =
    details?.code === undefined
      ? details?.message?.match(/\((100\d{2,3})\)\s*$/u)?.[1]
      : String(details.code);
  const reason = nativeReason(status, code);
  const rejected = [
    "NotFound",
    "AccessDenied",
    "PreconditionFailed",
    "RangeNotSatisfiable",
    "IntegrityMismatch",
    "Throttled",
    "InvalidInput",
    "Expired",
  ].includes(reason._tag);
  return storageError(
    operation,
    "r2-workers",
    reason,
    outcome === "unknown" && rejected ? "rejected" : outcome,
  );
};

export const call = <A>(
  operation: string,
  run: () => Promise<A>,
  outcome: MutationOutcome = "not-applicable",
) => Effect.tryPromise({ try: run, catch: bindingError(operation, outcome) });

export const metadata = Effect.fnUntraced(function* (storeId: string, object: R2ObjectLike) {
  const checksums = (["md5", "sha1", "sha256", "sha384", "sha512"] as const).flatMap(
    (algorithm) => {
      const bytes = object.checksums[algorithm];
      return bytes
        ? [
            {
              algorithm,
              encoding: "hex" as const,
              scope: "full-object" as const,
              value: Hex.encode(new Uint8Array(bytes)),
            },
          ]
        : [];
    },
  );
  const encoded = yield* Effect.try({
    try: () => ({
      ...object.httpMetadata,
      customMetadata: object.customMetadata,
      ref: { storeId, key: object.key },
      size: object.size,
      etag: object.httpEtag,
      lastModified: object.uploaded.toISOString(),
      checksums,
    }),
    catch: () =>
      storageError("metadata", "r2-workers", { _tag: "ProviderFailure", code: "InvalidMetadata" }),
  });
  return yield* Schema.decodeUnknownEffect(ObjectMetadata)(encoded).pipe(
    Effect.mapError(() =>
      storageError("metadata", "r2-workers", { _tag: "ProviderFailure", code: "InvalidMetadata" }),
    ),
  );
});

export const headers = (input: ObjectHeaders): Omit<R2PutOptions, "onlyIf"> => ({
  httpMetadata: {
    ...(input.contentType === undefined ? {} : { contentType: input.contentType }),
    ...(input.contentEncoding === undefined ? {} : { contentEncoding: input.contentEncoding }),
    ...(input.contentLanguage === undefined ? {} : { contentLanguage: input.contentLanguage }),
    ...(input.contentDisposition === undefined
      ? {}
      : { contentDisposition: input.contentDisposition }),
    ...(input.cacheControl === undefined ? {} : { cacheControl: input.cacheControl }),
  },
  ...(input.customMetadata ? { customMetadata: { ...input.customMetadata } } : {}),
});

export const validateKey = (key: string, operation: string) =>
  new TextEncoder().encode(key).length <= 1024
    ? Effect.void
    : Effect.fail(
        storageError(
          operation,
          "r2-workers",
          { _tag: "InvalidInput", field: "key" },
          operation === "put" || operation.startsWith("multipart")
            ? "not-started"
            : "not-applicable",
        ),
      );

/** Structured R2 conditions reject HTTP quotes; portable validators retain their wire spelling. */
export const conditionalEtag = (etag: string) =>
  etag.startsWith('"') && etag.endsWith('"') ? etag.slice(1, -1) : etag;
