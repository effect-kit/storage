import { Effect, Redacted } from "effect";

import { upload } from "@vercel/blob/client";

import { storageError } from "../errors.js";
import { providerError, validateKey } from "./mapping.js";

/** Native browser upload flow. Managed browser transfers and framework handlers are completed in phase 11. */
export const VercelClient = {
  upload: Effect.fnUntraced(function* (input: {
    readonly key: string;
    readonly file: Blob;
    readonly access: "public" | "private";
    readonly handleUploadUrl: string;
    readonly clientPayload?: string;
    readonly headers?: Redacted.Redacted<Readonly<Record<string, string>>>;
    readonly multipart?: boolean;
  }) {
    yield* validateKey(input.key, "client.upload");
    if (!(input.file instanceof Blob))
      return yield* storageError(
        "client.upload",
        "vercel",
        { _tag: "InvalidInput", field: "file" },
        "not-started",
      );
    return yield* Effect.tryPromise({
      try: (signal) =>
        upload(input.key, input.file, {
          access: input.access,
          handleUploadUrl: input.handleUploadUrl,
          abortSignal: signal,
          ...(input.clientPayload === undefined ? {} : { clientPayload: input.clientPayload }),
          ...(input.headers ? { headers: Redacted.value(input.headers) } : {}),
          multipart: input.multipart ?? false,
        }),
      catch: (error) => providerError("client.upload", true, error),
    });
  }),
};
