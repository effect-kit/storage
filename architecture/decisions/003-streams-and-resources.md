# 003 — Streaming, source types and resource ownership

Date: 2026-10-28. Decision: accepted from compiled/local probes.

## Decision

Canonical bodies use Effect Stream of Uint8Array. Upload operations preserve the
source error E and required environment R in their result types; adapters must
not erase them while converting to Node/Web streams.

Get acquires a scoped response containing metadata and a body from the same
request. Consumers read inside that scope. Storage.make also rejects a second
materialization and reading after the original scope closes. Adapters remain
responsible for native resource finalizers. Scope finalization disposes partially
consumed/unread bodies and aborts outstanding native work where supported.
Do not obtain metadata with a racing head request.

Library-created clients are acquired/released with their Layer. Borrowed clients
remain caller-owned. Provider promises receive the Effect interruption signal
when supported; body disposal is also necessary after response acquisition.
Native bindings without request abort support retain that limitation and may
leave an unknown mutation.

## Transfer requirements

Collection helpers require maxBytes, including unknown-size objects. Managed
transfers separately bound concurrency and retained bytes. Count source chunks,
part retries, checksums and SDK buffers; slicing a large backing buffer can retain
its entire allocation. Document caller-owned allocations separately.

Sources identify immutable bytes, reopenable verified files, explicit replay
factories or one-shot streams. Never resend a consumed source automatically.
Source fingerprint checks and durable protocol reconciliation belong to the
transfer/session phases.

## Evidence and consequences

The [probe](../../docs/evidence/phase-1.md) compiles preserved source E/R and scoped
SDK acquisition, exercises request interruption and finalizes a partially consumed
S3 response. Supabase REST exposes body/headers together; its SDK result does not.
Native SDK streams require resource bridges at adapter boundaries.

This establishes the ownership contract, not production memory bounds or every
SDK's cancellation semantics. Measure buffers/handles and test native source
failure/cleanup in the implementing phase before advertising those guarantees.

[Phase 2 evidence](../../docs/evidence/phase-2.md) covers source E/R, scoped and
single-use bodies, partial reads, memory reservations and bounded collection.
The memory limits describe logical retained bytes, not process RSS.
