import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";

import { assessReport } from "./report.ts";

const expected = {
  provider: "s3",
  suite: "portable",
  prefix: "effect-storage-acceptance/unit/run/",
} as const;
const evidence = {
  provider: "s3",
  status: "passed",
  suite: "portable",
  cleanupPending: 0,
  checks: ["put", "head", "verified-read", "scoped-list", "streamed-put", "managed-upload-sha256"],
  scopeDigest: createHash("sha256").update(expected.prefix).digest("hex"),
};

test("missing or incomplete child evidence cannot pass on a zero exit code", () => {
  assert.equal(assessReport(evidence, 0, expected).passed, true);
  for (const report of [
    undefined,
    {},
    { ...evidence, checks: [] },
    { ...evidence, checks: [...evidence.checks, "put"] },
  ])
    assert.equal(assessReport(report, 0, expected).passed, false);
  assert.equal(assessReport(evidence, 1, expected).passed, false);
});

test("provider, suite, namespace and cleanup debt bind success to the owned run", () => {
  for (const report of [
    { ...evidence, provider: "gcs" },
    { ...evidence, suite: "signing" },
    { ...evidence, scopeDigest: "0".repeat(64) },
    { ...evidence, cleanupPending: 1 },
    { ...evidence, status: "failed", reason: "ProviderCaseFailed" },
  ])
    assert.equal(assessReport(report, 0, expected).passed, false);
  const failed = {
    ...evidence,
    status: "failed",
    cleanupPending: 3,
    reason: "ProviderCaseOrCleanupFailed",
  };
  const assessed = assessReport(failed, 1, expected);
  assert("cleanupPending" in assessed.evidence);
  assert.equal(assessed.evidence.cleanupPending, 3);
  const signing = { ...evidence, suite: "signing", checks: [...evidence.checks, "signed-read"] };
  assert.equal(assessReport(signing, 0, { ...expected, suite: "signing" }).passed, true);
  assert.equal(
    assessReport({ ...signing, checks: evidence.checks }, 0, { ...expected, suite: "signing" })
      .passed,
    false,
  );
});

test("unexpected report fields and diagnostics never reach aggregate evidence", () => {
  const result = assessReport({ ...evidence, diagnostics: "secret-canary" }, 0, expected);
  assert.equal(result.passed, false);
  assert(!JSON.stringify(result).includes("secret-canary"));
});
