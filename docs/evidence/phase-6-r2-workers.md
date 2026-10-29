# Phase 6 R2 Workers evidence

Date: 2026-10-30, Darwin arm64 host. Native adapter uses Effect 4.1.0.
Local environment: Miniflare 4.20260730.0, workerd 1.20260730.1, compatibility date
2026-07-30. Official workers-types 5.20261029.1 is a development type check only.
No Cloudflare account, credential, remote bucket or deployed Worker was accessed.

`pnpm test:r2:workers` builds and packs the library, installs that archive in an
isolated app with only Effect and platform-browser, bundles an actual Worker while
rejecting Node/server SDK imports, disables outbound network and runs a disposable
local R2 binding. All thirteen portable conformance cases pass, including conditional
writes/reads, ranges, query-bound cursors and scoped single-use bodies.

Additional real-runtime checks pass:

- Known-length FixedLengthStream put with byte readback, original application source
  failure preservation short-length rejection and conditional stream rejection without a stranded producer.
- Native multipart initiate, two parts, stable same-number part re-upload, completion,
  abort and invalid reconstructed-upload failure on actual remote use.
- Explicit Unsupported part inventory and early conditional-multipart rejection.
- Managed multipart upload of 5 MiB plus 17 bytes and a pinned full-SHA-256 download.
  Verified streaming copy also passes; unsupported move rejects before any destination
  mutation and preserves its source. Logical admission stays within the configured 32 MiB budget and returns to zero.
- Web crypto protection and a shared-runtime memory-backed download session.
  Durable multipart upload creation fails closed without a provider handle codec.

The safe JSON report records runtime configuration, archive SHA-512, graph isolation,
conformance and resource counters in `.artifacts/r2-workers/report.json`. No checkpoint
plaintext, upload ID, key material or bearer URL is written into that report. The local
fixture's bucket resets and temporary state are owned and removed by the harness.

Default offline tests cover binding unions, null conditional puts, unread-body cleanup,
invalid ranges, bounded unknown-length sources, native safe error categories, unsupported
multipart before source reads, post-commit invalid receipt outcomes, explicit URL
resolution, writes continuing after cancellation and joined delayed read acquisition.
Official native binding assignment and SDK-free public declarations also compile.
`pnpm ci:check`, `pnpm test:extended` and actionlint validate source/package/workflow
contracts. The manual workflow was not dispatched remotely.

Hosted R2 is not qualified. No account environment or deployment authorization exists
in this session. Local emulation does not establish service consistency, same-key 429
races, deployed lifecycle/memory/ingress limits, power-cut durability or all compatibility
dates. Missing part/upload inventory, conditional multipart completion, native copy,
conditional deletion, signing/admin and distributed session ownership stay explicit
capability limits. See [the provider contract](../r2-workers.md).

The final combined phase 6 gate passes 142 package tests and 10 tooling tests,
format/catalog/lint/source/test/tooling checks, build and exact archive consumers.
All four actual runtime helper lanes pass again. Node/Bun filesystem acceptance,
compiled application examples and the existing local S3 lane (ten passes, one
AWS-only skip, process recovery/resource profiles) also pass. These do not close
the hosted R2 or unexecuted OS qualifications.
