# Phase 2 evidence

Date: 2026-10-28. Runtime: Node 24.22.0; Effect/@effect/vitest 4.0.6;
TypeScript 7.0.2; existing pnpm/Klarity/Vitest/tsdown versions retained.
No production or development dependency was added.

## Implemented and exercised

- Five guarded primitives, helpers and explicit conditions/ranges/read outcomes.
- JSON-safe object/metadata/native-reference codecs and safe error reasons.
- Memory storage with atomic publication, source-failure preservation, copied
  buffers/metadata, pinned read identity, clocks, quotas and bound cursors.
- Strict ordered fault scripts with response loss, body failures, delay/throttle,
  interruption, cleanup diagnostics and failure-safe observer simulation.
- A public 13-case conformance runner with caller-owned scoped fixtures.
- Separate signing/public-URL/copy services and advanced protocol/lifecycle types.
  Advanced types do not claim provider implementations.

The package suite passes 45 behavioral tests in six files. Ten tooling tests
validate evidence/catalog coverage and archive policy. Source and test typechecking
includes compile-only assertions preserving upload E/R, download Scope, separate
signing requirements and multiple named storage services.

Packed artifact tests install core and /testing in an isolated SDK-free consumer.
They execute memory/fault tools, all 13 memory conformance cases, an independently
written public-contract adapter and its public-URL Layer. Its unsupported optional
capabilities are explicitly skipped. NodeNext and Bundler compile the actual packed
declarations and negative type fixtures. Publint and ATTW pass for the supported
ESM profile; legacy CommonJS/node10 resolution remains outside that profile.

## Checks

`pnpm format`, `pnpm ci:check` and `pnpm test:extended` exercise formatting,
catalogs, lint, source/test/tooling types, tests, build and exact local package
consumers. Package checks are credential-free and do not import ignored research.

Test implementations and public fixtures live under `packages/storage/test/`;
the real artifact orchestrator is `scripts/artifacts/check.ts`.

## Limits and handoffs

Only the memory reference is implemented as a provider. It is process-local,
supports current references rather than retained history, and computes no digests.
No cloud/provider protocol, signing authority, governance, multipart/resumable
implementation, managed-transfer resource budget or durable session guarantee is
inferred from these tests.

AWS/native adapters and their real protocol/hosted acceptance follow in phases 3
and 6–9. Managed transfers/retry/cleanup are phase 4; durable sessions phase 5;
versions/governance phase 10; final runtime/provider/hardening matrices phase 14.
No hosted, browser, Bun, Worker deployment, OS filesystem, soak or registry release
lane was required or run for the phase 2 core/memory implementation.
