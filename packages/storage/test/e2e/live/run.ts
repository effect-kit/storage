/* oxlint-disable no-await-in-loop -- Remote evidence is read under a hard byte limit. */
import { createHash } from "node:crypto";
import { writeFile } from "node:fs/promises";

import { Cause, Context, Effect, Exit, Layer, Option, Schema } from "effect";

import { Storage, StorageError, StorageSigning } from "@effect-kit/storage";
import { layer as runtime } from "@effect-kit/storage/node";
import { TransferDriver } from "@effect-kit/storage/transfers";

import type { ProviderSettings } from "../../../../../scripts/providers/configuration.ts";
import { providerLayer } from "./adapters.ts";
import { authoritySuite } from "./authority.ts";
import { browserSuite } from "./browser.ts";
import { recoverySuite } from "./recovery.ts";
import { safetySuite } from "./safety.ts";
import { portableSuite } from "./suite.ts";
import { transferSuite } from "./transfers.ts";

let raw = "";
for await (const chunk of process.stdin) {
  raw += chunk;
  if (Buffer.byteLength(raw) > 262144) throw new Error("Input limit");
}
const { settings, suite, reportPath, artifactDigest, challenge } = JSON.parse(raw) as {
  settings: ProviderSettings;
  suite: string;
  reportPath: string;
  artifactDigest: string;
  challenge: string;
};
const report: {
  provider: string;
  status: string;
  suite: string;
  checks: string[];
  cleanupPending: number;
  reason?: string;
  scopeDigest?: string;
  failure?: { operation: string; reason: string };
} = {
  provider: settings.provider,
  status: "failed",
  suite,
  checks: [],
  cleanupPending: 0,
};
try {
  if (settings.provider === "r2-workers") {
    const response = await fetch(String(settings.config.url), {
      method: "POST",
      headers: {
        authorization: `Bearer ${settings.secrets.TOKEN}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ prefix: settings.prefix, suite, challenge }),
      redirect: "error",
      signal: AbortSignal.timeout(120000),
    });
    if (!response.ok) throw new Error("Remote Worker rejected run");
    const reader = response.body?.getReader();
    if (!reader) throw new Error("Missing Worker evidence");
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        length += next.value.length;
        if (length > 32768) throw new Error("Oversized Worker evidence");
        chunks.push(next.value);
      }
    } finally {
      await reader.cancel();
    }
    const result = Schema.decodeUnknownSync(
      Schema.Struct({
        buildId: Schema.String,
        challenge: Schema.String,
        status: Schema.Literal("passed"),
        prefix: Schema.String,
        checks: Schema.Array(
          Schema.Literals([
            "put",
            "head",
            "verified-read",
            "create-only",
            "range",
            "scoped-list",
            "streamed-put",
            "managed-upload-sha256",
          ]),
        ),
        cleanupPending: Schema.Literal(0),
      }),
      { onExcessProperty: "error" },
    )(JSON.parse(Buffer.concat(chunks).toString()));
    if (
      result.buildId !== artifactDigest ||
      result.challenge !== challenge ||
      result.status !== "passed" ||
      result.prefix !== settings.prefix ||
      ![
        "put",
        "head",
        "verified-read",
        "scoped-list",
        "streamed-put",
        "managed-upload-sha256",
      ].every((check) => result.checks.includes(check as (typeof result.checks)[number])) ||
      result.cleanupPending !== 0
    )
      throw new Error("Invalid Worker evidence");
    report.checks = [...result.checks];
    report.status = "passed";
  } else {
    const effective =
      suite === "release" && settings.provider === "supabase"
        ? { ...settings, config: { ...settings.config, resumable: true } }
        : settings;
    const layer = await providerLayer(effective, suite);
    const program = Effect.scoped(
      Effect.gen(function* () {
        yield* portableSuite(
          layer,
          settings.prefix,
          report,
          suite === "portable" ? "portable" : "signing",
        );
        if (suite !== "release" && suite !== "browser") return;

        const context = yield* Layer.build(layer);
        const storage = Context.get(context, Storage);
        const signing = Option.getOrThrow(Context.getOption(context, StorageSigning));
        if (suite === "browser") {
          yield* browserSuite(storage, signing, settings.prefix, report);
          return;
        }

        yield* safetySuite(storage, settings.prefix, report);
        yield* authoritySuite(effective, storage, signing, report);
        const endpoint = Context.get(context, TransferDriver);
        yield* transferSuite(storage, endpoint, settings.prefix, report);
        yield* recoverySuite(effective, storage, endpoint, settings.prefix, report);
      }),
    ).pipe(
      Effect.provide(runtime),
      Effect.timeout(suite === "release" ? "480 seconds" : "120 seconds"),
    );
    const exit = await Effect.runPromiseExit(program);
    if (Exit.isSuccess(exit) && report.cleanupPending === 0) report.status = "passed";
    else {
      report.reason = "ProviderCaseOrCleanupFailed";
      if (Exit.isFailure(exit)) {
        const error = Option.getOrUndefined(Cause.findErrorOption(exit.cause));
        if (error instanceof StorageError)
          report.failure = { operation: error.operation, reason: error.reason._tag };
      }
    }
  }
} catch {
  report.reason = "ProviderCaseFailed";
}
report.scopeDigest = createHash("sha256").update(settings.prefix).digest("hex");
await writeFile(reportPath, JSON.stringify(report, null, 2), { mode: 0o600 });
if (report.status !== "passed") process.exitCode = 1;
