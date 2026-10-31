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
] as const;
const Report = Schema.Struct({
  provider: ProviderId,
  status: Schema.Literals(["passed", "failed"]),
  suite: Schema.Literals(["portable", "signing"]),
  checks: Schema.Array(Schema.Literals(checks)),
  cleanupPending: Schema.Natural,
  reason: Schema.optional(Schema.Literals(["ProviderCaseFailed", "ProviderCaseOrCleanupFailed"])),
  scopeDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/u)),
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
    if (expected.suite === "signing") required.push("signed-read");
    const completed = new Set<string>(evidence.checks);
    const passed =
      exitCode === 0 &&
      evidence.status === "passed" &&
      evidence.cleanupPending === 0 &&
      !evidence.reason &&
      completed.size === evidence.checks.length &&
      required.every((check) => completed.has(check));
    return { passed, evidence: { ...evidence, status: passed ? "passed" : "failed" } };
  } catch {
    return {
      passed: false,
      evidence: { provider: expected.provider, status: "failed", reason: "ChildReportUnavailable" },
    };
  }
}
