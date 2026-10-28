# 006 — Evidence, runtime claims, CI and releases

Date: 2026-10-28. Decision: accepted.

## Evidence

Maintain all 47 feature IDs and 16 distinct provider targets in validated catalogs.
Operation records bind provider/configuration/runtime, support, constraints,
implementation/test references and dated evidence with SDK/API versions.
Documentation/source/compile/local-protocol/hosted/runtime/artifact evidence are
distinct. An endpoint preset or passing emulator is never a cloud qualification.

Verified cloud operations require matching hosted evidence and implemented code/tests.
Memory/filesystem need real runtime evidence. Unknown is research-needed.
Unsupported-native needs sourced limitation evidence; unsupported-adapter remains
implementation debt. Deferred work records destination, owner, rationale and
explicit approval; unavailable accounts do not automatically reduce scope.

## Runtimes and verification

Node 24.22.0 is the workspace baseline, with pinned pnpm/Effect/TypeScript.
Bun, Workers and browser exports require separate runtime qualification in their
implementing phases. Installed engine metadata and successful typechecking are
insufficient to advertise an SDK/runtime combination.

Default CI checks catalogs, formatting, lint, production/test/tooling types,
offline tests and builds. Extended artifact checks install exact packed
production exports into isolated SDK-free consumers and check NodeNext/Bundler
declarations. Service, browser, hosted and long resource lanes stay manual and
exist only when real suites are implemented.

## Release boundary

Keep shared workflows on effect-kit/.github@v1. Phase 14 must replace the template
release:check/release:smoke commands with exact-plan/tarball and registry/provenance
verification for every publishable package. Current artifact tests do not
implement that release contract or prove publishing permission.

Source/build inputs, package contents and installed public behavior must agree.
No ignored research is a CI/runtime dependency. Publish/push requires actual owner
authorization at the remote step; local implementation does not imply it.
Record skipped lanes and limitations rather than manufacture a passing report.
