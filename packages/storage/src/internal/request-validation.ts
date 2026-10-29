import { Effect, Schema } from "effect";

import type { StorageCapabilities } from "../capabilities.js";
import { storageError } from "../errors.js";
import type { ObjectTarget } from "../requests.js";

export function decodeRequest<S extends Schema.Constraint>(
  schema: S,
  input: unknown,
  operation: string,
  provider: string,
  mutation: boolean = operation === "put" || operation === "delete",
) {
  return Schema.decodeUnknownEffect(schema, { onExcessProperty: "error" })(input).pipe(
    Effect.mapError(() =>
      storageError(
        operation,
        provider,
        { _tag: "InvalidInput", field: "request" },
        mutation ? "not-started" : "not-applicable",
      ),
    ),
  );
}

export function validateTarget(
  target: ObjectTarget,
  storeId: string,
  provider: string,
  operation: string,
  capabilities: StorageCapabilities,
) {
  if ("ref" in target && target.ref.storeId !== storeId) {
    return Effect.fail(
      storageError(
        operation,
        provider,
        { _tag: "InvalidInput", field: "ref.storeId" },
        operation === "delete" ? "not-started" : "not-applicable",
      ),
    );
  }

  if ("ref" in target && target.ref.version && capabilities.versionedReads === "none") {
    return Effect.fail(
      storageError(
        operation,
        provider,
        { _tag: "Unsupported", capability: "versioned-reference" },
        operation === "delete" ? "not-started" : "not-applicable",
      ),
    );
  }

  return Effect.void;
}
