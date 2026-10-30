import { Effect, Stream } from "effect";

import { StorageError } from "../errors.js";
import type { GetResult, PutRequest } from "../requests.js";
import type { S3StorageService } from "../s3.js";
import { deleteObjects } from "./bulk-delete.js";
import type { S3Connection } from "./config.js";
import { copyObject, copyPart } from "./copy.js";
import { multipart } from "./multipart.js";
import type { S3PutOptions } from "./primitives.js";
import { primitives, putObject } from "./primitives.js";

export function assemble(connection: S3Connection): S3StorageService {
  const native = {
    ...primitives(connection),
    ...connection,
    multipart: multipart(connection),
    copyObject: (input: Parameters<S3StorageService["copyObject"]>[0], source?: S3StorageService) =>
      copyObject(connection, input, source),
    copyPart: (input: Parameters<S3StorageService["copyPart"]>[0], source?: S3StorageService) =>
      copyPart(connection, input, source),
    deleteObjects: (input: Parameters<S3StorageService["deleteObjects"]>[0]) =>
      deleteObjects(connection, input),
    putObject: <E = never, R = never>(input: PutRequest<E, R>, options?: S3PutOptions) =>
      putObject(connection, input, options),
  };
  if (!connection.profile) return native;
  const provider = connection.profile.provider;
  const rename = <E>(error: E): E | StorageError =>
    error instanceof StorageError && error.provider === "s3"
      ? new StorageError({ ...error, provider })
      : error;
  const mapped = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.mapError(effect, rename);
  const uploads = native.multipart;
  const renameBody = (result: GetResult) =>
    result._tag === "Object" ? { ...result, body: Stream.mapError(result.body, rename) } : result;

  return {
    ...native,
    put: (input) => mapped(native.put(input)),
    putObject: (input, options) => mapped(native.putObject(input, options)),
    head: (input) => mapped(native.head(input)),
    get: (input) => mapped(native.get(input)).pipe(Effect.map(renameBody)),
    delete: (input) => mapped(native.delete(input)),
    listPage: (input) => mapped(native.listPage(input)),
    copyObject: (input, source) => mapped(native.copyObject(input, source)),
    copyPart: (input, source) => mapped(native.copyPart(input, source)),
    deleteObjects: (input) => mapped(native.deleteObjects(input)),
    multipart: {
      ...uploads,
      initiate: (input) => mapped(uploads.initiate(input)),
      uploadPart: (input) => mapped(uploads.uploadPart(input)),
      complete: (input) => mapped(uploads.complete(input)),
      abort: (input) => mapped(uploads.abort(input)),
      listParts: (input) => mapped(uploads.listParts(input)),
      listUploads: (input) => mapped(uploads.listUploads(input)),
    },
  };
}
