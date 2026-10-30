import { Predicate } from "effect";

import { storageError } from "../../errors.js";

/** SDK errors may contain credentials and bearer URLs; retain only classified status. */
export function providerError(
  provider: string,
  operation: string,
  mutation: boolean,
  error: unknown,
) {
  const status = Predicate.isObject(error)
    ? (Reflect.get(error, "statusCode") ?? Reflect.get(error, "code"))
    : undefined;
  const code = Number(status);
  const reason =
    code === 304
      ? { _tag: "NotModified" as const }
      : code === 404
        ? { _tag: "NotFound" as const }
        : code === 401 || code === 403
          ? { _tag: "AccessDenied" as const }
          : code === 409 || code === 412
            ? { _tag: "PreconditionFailed" as const }
            : code === 416
              ? { _tag: "RangeNotSatisfiable" as const }
              : code === 429
                ? { _tag: "Throttled" as const }
                : Number.isInteger(code) && code >= 400
                  ? { _tag: "ProviderFailure" as const, code: String(code) }
                  : { _tag: "TransportFailure" as const };
  return storageError(
    operation,
    provider,
    reason,
    mutation ? (code >= 400 && code < 500 ? "rejected" : "unknown") : "not-applicable",
  );
}
