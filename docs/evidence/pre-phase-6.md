# Workflow/runtime redesign evidence

Local verification dated 2026-10-30, Darwin arm64, Effect/platform/test packages 4.1.0.
The library is unpublished. This record covers the preparation between phases 5 and 6;
it does not qualify the filesystem object provider or native R2 binding.

## Implemented contracts

Provider layers supply TransferDriver and optional native handle encoding. Transfers
captures runtime dependencies with policy-only make/layer. UploadSessions captures the
managed companion and store, sharing admission/request resources. Core Storage is
unchanged. Canonical runtime modules replace three development paths without aliases.
See [ADR 008](../../architecture/decisions/008-workflow-runtime-layers.md).

## Checks and artifacts

- `pnpm format` and `pnpm ci:check`: formatting, all 47 feature/16 provider catalog
  entries, lint, source/test/tooling types, 122 package tests, 10 tooling tests and build.
- `pnpm test:extended`: Publint/Are the Types Wrong, exact archive policy, isolated
  core/testing and S3 production consumers, NodeNext/Bundler declarations and source E/R.
- `pnpm test:runtimes`: actual engines below; each isolated exact archive installs only
  its selected platform peer (Node also satisfies upstream's Redis client peer).
  Both declaration modes run without Node ambient types or any provider SDK installed.
- `pnpm test:s3:local`: ten passes and one AWS-only skip; owned RustFS, fresh-process
  upload/download recovery and the existing 320 MiB/32 MiB logical admission profile.
- `actionlint .github/workflows/*.yml`: local workflow syntax/expression check.

| Actual engine                            | Result | Exercised behavior                                                                                                    |
| ---------------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------- |
| Node 24.22.0                             | Passed | Native hash/AES, managed memory driver, protected download, actual files/source/publication and reconstructed backing |
| Bun 1.4.4                                | Passed | Same helper fixture under Bun with official Bun filesystem/crypto layers                                              |
| Deno 2.9.6                               | Passed | Portable hash, Effect Aead, actual Deno permissions/resolution/files and reconstructed backing                        |
| Chromium 156.0.8078.4, Playwright 1.64.1 | Passed | Portable hash/AES, managed memory driver, protected in-memory download; server-free browser bundle                    |

The Node/Bun/Deno helper fixture checks incremental SHA-256 and non-consuming prefix
snapshots, AES associated-data rejection, upload/download bytes/checksum, synced file
publication, replayable file source, unsupported inventory, reconstructed checkpoint
state and idempotent final publication. Browser uses a custom memory target/KVS and
has no filesystem exports. Safe reports are written to `.artifacts/runtime/*.json`.

New unit regressions cover native/portable hash parity (empty, subarray, multi-chunk
and snapshot cases), native/web AES envelope compatibility, cancellation joining an
issued file write, driver identity validation, unsupported upload codecs before source
opening, and mixed managed/session byte/request ceilings without nested admission
deadlock. Existing outcome, expiry, attempts, events, scopes and source-change tests
remain in the offline suite.

The S3 subprocess matrix retains fifteen upload/abort/event and three download/
publication crash boundaries. Reports are `.artifacts/session-restart.json`,
`.artifacts/transfers-resources.json` and the provider report. These are fresh Node
processes and owned local protocol tests; no cloud account is used.

## Limits and remaining gates

Bun/Deno reconstruct backing in one process; neither has a fresh-process crash matrix
here. Browser has no reload/IndexedDB, cross-tab ownership, mobile/browser matrix or
direct-upload UI evidence. The new manual runtime workflow is locally linted but was
not dispatched; Ubuntu/remote success is not inferred from Darwin execution.

Hosted AWS remains explicitly postponed by the owner. Runtime helper success does not
certify S3 transport on Bun/Deno/browser, deployed Workers, other providers, Windows,
network filesystems, power-cut/soak behavior or distributed fencing. Exclusive
ownership, source equivalence, stable keys, known IDs and receiver deduplication remain
application contracts. Release check/smoke commands are still placeholders; no release,
registry installation, provenance or publishing claim follows from packed consumers.
