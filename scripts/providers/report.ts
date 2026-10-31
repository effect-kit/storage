import { createHash } from "node:crypto";

import { Schema } from "effect";

import { ProviderId } from "./configuration.ts";

const checks = [
  "put",
  "head",
  "verified-read",
  "create-only",
  "range",
  "scoped-list",
  "streamed-put",
  "managed-upload-sha256",
  "signed-read",
  "missing-object",
  "foreign-reference-denied",
  "concurrent-create-only",
  "create-only-unsupported",
  "stale-write-rejected",
  "conditional-overwrite-unsupported",
  "invalid-credential-denied",
  "expired-grant-denied",
  "large-native-transfer",
  "lost-part-response-retried",
  "offset-transfer",
  "checksum-mismatch-rejected",
  "cancelled-multipart-cleaned",
  "multipart-cancellation-unavailable",
  "transfer-resources-released",
  "fresh-process-upload-recovery",
  "durable-upload-unavailable",
  "browser-signed-read",
] as const;
const Report = Schema.Struct({
  provider: ProviderId,
  status: Schema.Literals(["passed", "failed"]),
  suite: Schema.Literals(["portable", "signing", "release", "browser"]),
  checks: Schema.Array(Schema.Literals(checks)),
  cleanupPending: Schema.Natural,
  reason: Schema.optional(Schema.Literals(["ProviderCaseFailed", "ProviderCaseOrCleanupFailed"])),
  scopeDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/u)),
  failure: Schema.optional(
    Schema.Struct({
      operation: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9.-]{1,80}$/u)),
      reason: Schema.Literals([
        "InvalidInput",
        "InvalidConfiguration",
        "Unsupported",
        "NotFound",
        "AccessDenied",
        "PreconditionFailed",
        "Expired",
        "Throttled",
        "TransportFailure",
        "ProviderFailure",
        "IntegrityMismatch",
        "LengthMismatch",
        "LimitExceeded",
        "InvalidSession",
        "QuotaExceeded",
        "ScopeClosed",
        "BodyConsumed",
        "PartialFailure",
        "NotModified",
        "RangeNotSatisfiable",
        "AssertionFailed",
      ]),
    }),
  ),
});

export function assessReport(
  input: unknown,
  exitCode: number,
  expected: { readonly provider: ProviderId; readonly suite: string; readonly prefix: string },
) {
  try {
    const evidence = Schema.decodeUnknownSync(Report, { onExcessProperty: "error" })(input);
    const scope = createHash("sha256").update(expected.prefix).digest("hex");
    if (
      evidence.provider !== expected.provider ||
      evidence.suite !== expected.suite ||
      evidence.scopeDigest !== scope
    )
      throw new Error("Report identity mismatch");

    const required = [
      "put",
      "head",
      "verified-read",
      "scoped-list",
      "streamed-put",
      "managed-upload-sha256",
    ];
    if (expected.suite !== "portable") required.push("signed-read");
    if (expected.suite === "browser") required.push("browser-signed-read");
    if (expected.suite === "release") {
      required.push(
        "missing-object",
        "foreign-reference-denied",
        "invalid-credential-denied",
        "expired-grant-denied",
        "large-native-transfer",
        "checksum-mismatch-rejected",
        "transfer-resources-released",
      );
      if (expected.provider === "vercel")
        required.push(
          "lost-part-response-retried",
          "multipart-cancellation-unavailable",
          "durable-upload-unavailable",
        );
      else if (expected.provider === "supabase")
        required.push(
          "offset-transfer",
          "multipart-cancellation-unavailable",
          "fresh-process-upload-recovery",
        );
      else
        required.push(
          "lost-part-response-retried",
          "cancelled-multipart-cleaned",
          "fresh-process-upload-recovery",
        );
    }
    const completed = new Set<string>(evidence.checks);
    const passed =
      exitCode === 0 &&
      evidence.status === "passed" &&
      evidence.cleanupPending === 0 &&
      !evidence.reason &&
      !evidence.failure &&
      completed.size === evidence.checks.length &&
      (expected.suite !== "release" ||
        [
          ["concurrent-create-only", "create-only-unsupported"],
          ["stale-write-rejected", "conditional-overwrite-unsupported"],
          ["lost-part-response-retried", "offset-transfer"],
          ["cancelled-multipart-cleaned", "multipart-cancellation-unavailable"],
          ["fresh-process-upload-recovery", "durable-upload-unavailable"],
        ].every(
          (alternatives) => alternatives.filter((check) => completed.has(check)).length === 1,
        )) &&
      required.every((check) => completed.has(check));
    return { passed, evidence: { ...evidence, status: passed ? "passed" : "failed" } };
  } catch {
    return {
      passed: false,
      evidence: { provider: expected.provider, status: "failed", reason: "ChildReportUnavailable" },
    };
  }
}
