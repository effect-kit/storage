# Phase 5 evidence — Effect persistence and restart recovery

Date: 2026-10-30. Node 24.22.0, Effect/@effect/vitest 4.0.6,
TypeScript 7.0.2, official AWS SDK 3.1159.0; local Darwin arm64 workspace.

The owner approved replacing mandatory PostgreSQL with Effect persistence and
explicit exclusive ownership on 2026-10-30. The implementation reuses
KeyValueStore.toSchemaStore and schema JSON codecs. Persistence.Persistable stores
request Exits; it is not an atomic mutable-session coordinator. No dependency,
SQL migration, Redis service or distributed lease implementation was added.

## Executed checks

- `pnpm format` and `pnpm ci:check`: formatting, 47-feature/16-provider catalogs,
  lint, source/test/tooling types, 114 package tests, 10 tooling tests and builds.
- `pnpm test:extended`: strict Publint/ATTW ESM and isolated exact archives;
  SDK-free sessions imports/runtime and NodeNext/Bundler declarations preserve
  source/event/batch callback environments. Optional Node helpers do not require
  Node typings in generic consumers. Existing S3/signing/POST peers remain isolated.
- `pnpm test:s3:local`: builds current JS for subprocess fixtures and runs owned
  versioned RustFS acceptance. Ten tests pass, one AWS-only authority/region test
  is skipped. Existing 13 portable conformance cases and phase 4 resource tests
  remain included. No AWS account was accessed.

Offline session cases cover protected/schema-versioned records, corrupt/future
versions without deletion, associated-data isolation, changed source/configuration,
concurrent service finalization, stale revisions, expiry/cleanup, source mutation
between fingerprint and part reads, per-part source binding, response-loss
reconciliation, definitive rejection retry, durable events, batches, target identity,
uncheckpointed target tails, retention, real disk reconstruction and AES-GCM keys.

## Separate-process crash matrix

`test/provider/session-worker/run.mjs` runs against the owned local bucket with
an encrypted atomic-file checkpoint store. The parent terminates child execution
with process.exit at explicit boundaries; each recovery is a fresh Node process.
Generated fixture keys/credentials are ephemeral and are never recorded in reports.

| Crash boundary                                                   | Observed recovery                                      |
| ---------------------------------------------------------------- | ------------------------------------------------------ |
| Created record saved / caller receives created                   | Resumes and completes                                  |
| Immediately before/after remote initiation without saved handle  | Initiating/orphan debt retained                        |
| Before/after part response or saved receipt                      | Reconciles inventory; saved parts are not resent       |
| Before completion request after durable intent                   | Completion-unknown retained; no blind replay           |
| Remote complete success before checkpoint                        | Marker proof, verified readback, completed             |
| Before verification / before or after completed save             | Finishes or returns existing completion                |
| Event receiver success before delivery acknowledgment            | Same event identity; receiver deduplicates             |
| Immediately before/after remote abort                            | Handle cleanup or inventory absence, aborted           |
| Download target write before checkpoint                          | Truncates uncheckpointed tail and resumes pinned bytes |
| Verified download checkpoint / publication before acknowledgment | Idempotent verified publication, completed             |

Every recovered successful upload has exactly one provider version at its target;
completion is not replayed to silently produce another version. The event fixture
retains one receiver record across replay. Download fixtures compare final bytes
exactly. Reports are `.artifacts/session-restart.json` and the aggregate
`.artifacts/s3-acceptance.json`. The worker/fixture removes its owned local files,
versions, uploads, container and temporary credentials.

These tests deliberately retain uncertainty where generic inventory cannot prove
an outcome. They do not establish automatic recovery at every possible boundary.
The native S3 handle codec preserves checksum policy; later native protocols need
their own observed-handle/reconciliation implementations.

## Guarantee boundaries and later qualification

- Base ownership is application-exclusive across processes. KVS modify and the
  repository's revision check do not become distributed CAS. A stronger ownership
  hook is an application contract, not a shipped/qualified lease backend.
- File and directory fsync/rename ordering is implemented and process restart is
  exercised on this OS. There was no physical power-cut, hostile-root writer,
  Windows, network-filesystem or other OS qualification. Phase 6/14 retain their gates.
- Required events use stable identities and at-least-once delivery. Exactly-once
  application side effects require receiver deduplication; there is no atomic
  checkpoint/cloud/application-database transaction.
- Source scans and full object verification are explicit costs. Arbitrary streams,
  unknown-length inputs and unowned sinks cannot promise restart recovery.
- Checkpoints have a separate 32 MiB plaintext limit by default; Node stored records
  are capped at 64 MiB. Backend/codec serialization is not a process RSS ceiling.
  Durable multipart execution is sequential; phase 4 managed transfers retain their
  parallel streaming path. No new cloud throughput claim is made.
- Hosted AWS remains owner-postponed. Offset/block/tus/Worker integration retains
  phases 6–9. Optional distributed coordination must be separately implemented and
  qualified before it is advertised; XFR-04 retains that unqualified extension.
