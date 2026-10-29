# 005 — Durable persistence, filesystem trust and local fixtures

Date: 2026-10-28. Decision: accepted; exclusive Effect KVS implementation revised/delivered 2026-10-30.

## Persistence

Keep core independent of persistence. Updated 2026-10-30 by owner request: reuse
Effect persistence APIs (now pinned to 4.1.0) and make PostgreSQL/distributed coordination optional.
KeyValueStore.toSchemaStore provides typed mutable values; Persistence stores
Persistable request Exits and TTLs. Neither public interface supplies session CAS
or worker leases. Never substitute KVS read-then-write for distributed fencing.

Phase 5 uses versioned protected checkpoints over application-provided KeyValueStore,
with explicit application-exclusive ownership across processes and in-process
serialization. A narrow ownership extension can wrap an application's stronger
coordinator; its guarantees are not invented by the base adapter. The shared runtime filesystem
backing adds atomic replacement/fsync; fresh-process restart is qualified on Node.
[ADR 008](008-workflow-runtime-layers.md) records canonical runtime composition and
the Bun/Deno helper evidence boundary. Applications
must authorize access and prevent concurrent independent processes for the same
namespace. PostgreSQL is an optional future integration rather than a phase gate.

No checkpoint transaction atomically commits a cloud write. Persist intent before
external operations, reconcile uncertain results, and protect all sensitive handles
through an application codec. Never serialize credentials. The earlier PostgreSQL
17.7 probe remains historical evidence, not a required production dependency.

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
