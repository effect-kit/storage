import { Clock, Effect, Predicate } from "effect";

import { StorageError, storageError } from "../errors.js";
import type { StorageReason } from "../errors.js";
import type { S3Connection } from "./config.js";

const knownReasons: Readonly<Record<string, StorageReason>> = {
  NoSuchKey: { _tag: "NotFound" },
  NoSuchVersion: { _tag: "NotFound" },
  NotFound: { _tag: "NotFound" },
  AccessDenied: { _tag: "AccessDenied" },
  InvalidAccessKeyId: { _tag: "AccessDenied" },
  SignatureDoesNotMatch: { _tag: "AccessDenied" },
  PreconditionFailed: { _tag: "PreconditionFailed" },
  NotModified: { _tag: "NotModified" },
  InvalidRange: { _tag: "RangeNotSatisfiable" },
  ExpiredToken: { _tag: "Expired" },
  RequestExpired: { _tag: "Expired" },
  SlowDown: { _tag: "Throttled" },
  Throttling: { _tag: "Throttled" },
  BadDigest: { _tag: "IntegrityMismatch" },
  InvalidDigest: { _tag: "IntegrityMismatch" },
  NoSuchUpload: { _tag: "InvalidSession" },
};

export function mapS3Error(
  cause: unknown,
  operation: string,
  mutation: boolean,
  nowMillis?: number,
): StorageError {
  if (cause instanceof StorageError) return cause;
  const failure = Predicate.isObject(cause) ? cause : {};
  const name = typeof failure.name === "string" ? failure.name : "";
  const metadata = Predicate.isObject(failure.$metadata) ? failure.$metadata : {};
  const status = typeof metadata.httpStatusCode === "number" ? metadata.httpStatusCode : undefined;
  const requestId =
    typeof metadata.requestId === "string" && /^[A-Za-z0-9+/=_-]{1,256}$/u.test(metadata.requestId)
      ? metadata.requestId
      : undefined;
  let reason: StorageReason;
  const known = Object.hasOwn(knownReasons, name) ? knownReasons[name] : undefined;
  if (typeof failure.message === "string" && failure.message.startsWith("Checksum mismatch:")) {
    reason = { _tag: "IntegrityMismatch" };
  } else if (known) {
    reason = known;
  } else {
    switch (status) {
      case 304:
        reason = { _tag: "NotModified" };
        break;
      case 403:
        reason = { _tag: "AccessDenied" };
        break;
      case 412:
        reason = { _tag: "PreconditionFailed" };
        break;
      case 416:
        reason = { _tag: "RangeNotSatisfiable" };
        break;
      case 429:
        reason = { _tag: "Throttled" };
        break;
      case undefined:
        reason = { _tag: "TransportFailure" };
        break;
      default:
        reason = {
          _tag: "ProviderFailure",
          ...(/^[A-Za-z][A-Za-z0-9]{0,63}$/u.test(name) ? { code: name } : {}),
        };
    }
  }

  if (reason._tag === "Throttled") {
    const response = Predicate.isObject(failure.$response) ? failure.$response : {};
    const headers = Predicate.isObject(response.headers) ? response.headers : {};
    const retryAfter = headers["retry-after"];
    if (typeof retryAfter === "string") {
      const millis = /^\d+(?:\.\d+)?$/u.test(retryAfter)
        ? Number(retryAfter) * 1000
        : nowMillis === undefined
          ? Number.NaN
          : Date.parse(retryAfter) - nowMillis;
      if (Number.isFinite(millis) && millis <= Number.MAX_SAFE_INTEGER)
        reason = { _tag: "Throttled", retryAfterMillis: Math.max(0, Math.ceil(millis)) };
    }
  }

  // Generic 404s can mean a missing bucket or hidden authority; only named object errors prove absence.
  return new StorageError({
    operation,
    provider: "s3",
    reason,
    requestId,
    mutationOutcome: mutation
      ? (reason._tag === "Throttled" && status === 503) ||
        (status !== undefined && status >= 400 && status < 500 && status !== 408)
        ? "rejected"
        : "unknown"
      : "not-applicable",
  });
}

export function request<A>(
  connection: S3Connection,
  operation: string,
  mutation: boolean,
  execute: (signal: AbortSignal) => Promise<A>,
) {
  return Effect.tryPromise({
    try: execute,
    catch: (cause) => cause,
  }).pipe(
    Effect.catch((cause) =>
      Effect.flatMap(Clock.currentTimeMillis, (now) =>
        Effect.fail(
          connection.profile
            ? new StorageError({
                ...mapS3Error(cause, operation, mutation, now),
                provider: connection.profile.provider,
              })
            : mapS3Error(cause, operation, mutation, now),
        ),
      ),
    ),
    Effect.timeoutOrElse({
      duration: connection.config.requestTimeoutMillis,
      orElse: () =>
        Effect.fail(
          storageError(
            operation,
            "s3",
            { _tag: "TransportFailure", code: "DeadlineExceeded" },
            mutation ? "unknown" : "not-applicable",
          ),
        ),
    }),
  );
}
