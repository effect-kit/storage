import type { PassThrough } from "node:stream";

import type { Effect } from "effect";

import type { StorageError } from "../../core/errors.js";
import type { PutRequest } from "../../core/requests.js";
import { uploadBody as nodeUploadBody } from "../../internal/node/upload-body.js";
import type { S3Connection } from "./config.js";

export const uploadBody = <A, E, R>(
  connection: S3Connection,
  input: Pick<PutRequest<E, R>, "body" | "contentLength" | "maxBytes">,
  operation: string,
  execute: (body: Uint8Array | PassThrough, length: number) => Effect.Effect<A, StorageError>,
) =>
  nodeUploadBody(
    { provider: "s3", maxUnknownLengthBytes: connection.config.maxUnknownLengthBytes },
    input,
    operation,
    execute,
  );
