/* oxlint-disable no-await-in-loop -- Request chunks are bounded and decoded in order. */
import { Effect, Exit, Schema } from "effect";

import type { R2Bucket } from "@cloudflare/workers-types";
import { layer as runtime } from "@effect-kit/storage/browser";
import { R2WorkersStorage } from "@effect-kit/storage/r2-workers";

import { portableSuite } from "./suite.ts";

declare const storageLiveBuildId: string;
interface Bindings {
  BUCKET: R2Bucket;
  STORAGE_TEST_TOKEN: string;
  STORAGE_TEST_PREFIX: string;
}
const Input = Schema.Struct({
  prefix: Schema.String.check(
    Schema.isPattern(/^effect-storage-acceptance\/[A-Za-z0-9_-]+\/[a-f0-9-]{36}\/$/u),
  ),
  suite: Schema.Literal("portable"),
  challenge: Schema.NonEmptyString,
});
export default {
  async fetch(request: Request, env: Bindings) {
    if (
      request.method !== "POST" ||
      !env.STORAGE_TEST_TOKEN ||
      request.headers.get("authorization") !== `Bearer ${env.STORAGE_TEST_TOKEN}`
    )
      return new Response(null, { status: 403 });
    if (!env.STORAGE_TEST_PREFIX || !env.BUCKET) return new Response(null, { status: 503 });
    let input;
    try {
      const reader = request.body?.getReader();
      if (!reader) throw new Error("Body required");
      let text = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        text += new TextDecoder().decode(value);
        if (text.length > 2048) {
          await reader.cancel();
          throw new Error("Body too large");
        }
      }
      input = Schema.decodeUnknownSync(Input, { onExcessProperty: "error" })(JSON.parse(text));
      if (
        !/^effect-storage-acceptance\/[A-Za-z0-9_-]+\/$/u.test(env.STORAGE_TEST_PREFIX) ||
        !input.prefix.startsWith(env.STORAGE_TEST_PREFIX) ||
        !/^[a-f0-9-]{36}\/$/u.test(input.prefix.slice(env.STORAGE_TEST_PREFIX.length))
      )
        throw new Error("Scope mismatch");
    } catch {
      return new Response(null, { status: 400 });
    }
    const report = {
      buildId: storageLiveBuildId,
      challenge: input.challenge,
      status: "failed",
      prefix: input.prefix,
      cleanupPending: 0,
      checks: [] as string[],
    };
    const layer = R2WorkersStorage.layer({ bucket: env.BUCKET, storeId: "live-worker-binding" });
    const exit = await Effect.runPromiseExit(
      Effect.scoped(portableSuite(layer, input.prefix, report)).pipe(Effect.provide(runtime)),
    );
    if (Exit.isSuccess(exit) && report.cleanupPending === 0) report.status = "passed";
    return Response.json(report, { status: report.status === "passed" ? 200 : 500 });
  },
};
