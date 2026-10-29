import { Effect, Result } from "effect";

import { Storage } from "@effect-kit/storage";
import { R2WorkersStorage } from "@effect-kit/storage/r2-workers";

// Bind STORAGE to your bucket in the application; authorize requests before this handler.
export default {
  async fetch(_request, env) {
    const read = Storage.getText({ key: "example/hello.txt", maxBytes: 1024 * 1024 }).pipe(
      Effect.provide(R2WorkersStorage.layer({ bucket: env.STORAGE, storeId: env.STORAGE_ID })),
      Effect.result,
    );
    const result = await Effect.runPromise(read);
    if (Result.isSuccess(result))
      return new Response(result.success, {
        headers: { "content-type": "text/plain; charset=utf-8" },
      });
    return new Response("Object unavailable", {
      status: result.failure.reason._tag === "NotFound" ? 404 : 502,
    });
  },
};
