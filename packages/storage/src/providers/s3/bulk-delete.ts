import { Effect, Result, Schema } from "effect";

import { DeleteObjectsCommand } from "@aws-sdk/client-s3";

import { storageError } from "../../core/errors.js";
import type { StorageError } from "../../core/errors.js";
import { DeleteRequest } from "../../core/requests.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { authority } from "./config.js";
import type { S3Connection } from "./config.js";
import { target, validateRef } from "./object-mapping.js";
import { enforceProfile } from "./profile.js";
import { mapS3Error, request } from "./transport.js";

export interface S3DeleteOutcome {
  readonly request: DeleteRequest;
  readonly result: Result.Result<void, StorageError>;
}

export const deleteObjects = Effect.fnUntraced(function* (
  connection: S3Connection,
  inputs: ReadonlyArray<DeleteRequest>,
) {
  const values = yield* decodeRequest(
    Schema.Array(DeleteRequest).check(Schema.isMinLength(1), Schema.isMaxLength(1000)),
    inputs,
    "deleteObjects",
    "s3",
  );
  const identities = new Set<string>();
  for (const value of values) {
    yield* enforceProfile(connection, "deleteObjects", value);
    if ("ref" in value) yield* validateRef(value.ref, connection.config, "deleteObjects");
    if (value.condition)
      return yield* storageError(
        "deleteObjects",
        "s3",
        { _tag: "Unsupported", capability: "batch-delete-condition" },
        "not-started",
      );
    const identity = JSON.stringify(target(value));
    if (identities.has(identity))
      return yield* storageError(
        "deleteObjects",
        "s3",
        { _tag: "InvalidInput", field: "duplicate-target" },
        "not-started",
      );
    identities.add(identity);
  }

  const response = yield* Effect.result(
    request(connection, "deleteObjects", true, (signal) =>
      connection.client.send(
        new DeleteObjectsCommand({
          ...authority(connection.config),
          Delete: { Objects: values.map((value) => target(value)), Quiet: false },
        }),
        { abortSignal: signal },
      ),
    ),
  );
  if (Result.isFailure(response))
    return values.map((value) => ({ request: value, result: Result.fail(response.failure) }));

  return values.map((value): S3DeleteOutcome => {
    const selected = target(value);
    const error = response.success.Errors?.find(
      (entry) => entry.Key === selected.Key && entry.VersionId === selected.VersionId,
    );
    const deleted = response.success.Deleted?.some(
      (entry) => entry.Key === selected.Key && entry.VersionId === selected.VersionId,
    );
    const result = error
      ? Result.fail(
          mapS3Error({ name: error.Code, $metadata: { httpStatusCode: 400 } }, "delete", true),
        )
      : deleted
        ? Result.succeed(undefined)
        : Result.fail(
            storageError(
              "delete",
              "s3",
              { _tag: "ProviderFailure", code: "MissingDeleteReceipt" },
              "unknown",
            ),
          );
    return { request: value, result };
  });
});
