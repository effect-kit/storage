# 005 — Durable persistence, filesystem trust and local fixtures

Date: 2026-10-28. Decision: accepted for subsequent implementation.

## Persistence

Keep core independent of persistence. The first production UploadSessionStore
uses compatible Effect SQL/PostgreSQL behind an optional subpath. Reuse SqlClient
transactions/migrations; implement focused revision-CAS, lease and transition
queries. Effect KeyValueStore/Persistence expose useful get/set/TTL facilities
but their public contracts do not provide the required session CAS/lease protocol.
A generic key/value read-then-write cannot substitute for it.

The Effect 4.0.6 / @effect/sql-pg 4.0.6 local probe connects to PostgreSQL 17.7,
performs a conditional revision update and verifies transaction rollback.
Distributed claims, fencing, lease renewal and durable resume are not implemented.
No database transaction atomically commits a cloud upload; persist intent and
reconcile each external/state crash window. Protect bearer handles through an
application-supplied secret codec/reference store, never checkpoint credentials.

## Filesystem threat model

Select a trusted application-owned root with hostile object keys and ordinary
application concurrency. Hostile concurrent local filesystem writers and root
replacement are outside the baseline guarantee. Phase 6 must test confinement,
symlinks, publication, permissions and OS behavior under that stated boundary.

Bytes and metadata need a single-generation publication design; independently
renaming a data file and sidecar is insufficient. Implement/test the concrete
layout and supported atomic primitives in phase 6. Reject unavailable cross-process
CAS guarantees; do not claim them through a process-local mutex or stat-then-write.

## Local fixture selection

Use RustFS 1.0.4 as the generic local S3 protocol fixture:
`rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2`.
Its Apache-2.0 license and current release were checked in the
[official release](https://github.com/rustfs/rustfs/releases/tag/1.0.4) and
[license](https://github.com/rustfs/rustfs/blob/1.0.4/LICENSE).
PostgreSQL uses image digest
`sha256:2006493727bd5277eece187319af1ef82b4cf82cf4fc1ed00da0775b646ac2a4`.

Fixtures bind loopback ephemeral ports, use generated disposable credentials,
owned tmpfs data and explicit container cleanup. The local probes do not certify
AWS, R2 or MinIO. MinIO remains a named target qualified against an explicit
actual edition/version in phase 9; the community archive finding is a
maintenance decision input, not permission to rename RustFS test evidence.
