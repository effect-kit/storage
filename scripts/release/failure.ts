/** Report actionable failure identity without copying child output, credentials or candidate contents. */
export function releaseFailure(error: unknown, stage: string) {
  const failure = error instanceof Error ? error : undefined;
  const output = failure && "stdout" in failure ? String(failure.stdout) : "";
  const diagnostics = failure && "stderr" in failure ? String(failure.stderr) : "";
  const pnpm = `${output}\n${diagnostics}`.match(/\bERR_PNPM_[A-Z_]{1,80}\b/u)?.[0];
  const code =
    failure &&
    "code" in failure &&
    typeof failure.code === "string" &&
    /^[A-Z_]{1,80}$/u.test(failure.code)
      ? failure.code
      : undefined;
  return { stage, code: pnpm ?? code ?? "CHECK_FAILED" };
}
