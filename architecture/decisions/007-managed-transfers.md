# 007 — Managed transfer ownership

Status: implemented for in-process numbered multipart and portable streaming paths.

The Effect-only transfers entry point coordinates narrow provider hooks. Official
SDKs execute one attempt; the engine owns retry/deadline accounting. Node crypto is
isolated in `/transfers/node`; S3 hook construction stays in `/s3/transfers`. Core
imports remain free of provider clients and Node crypto.

Reserve whole workspaces through FIFO admission before source reads. Keep request
concurrency separate and hold request permits through body consumption. Retain
bounded numbered part buffers across retry and validate retained completion identities.
Server-side part copy reserves control/transport state rather than payload buffers.
Reservations express engine admission, not a process RSS guarantee. Caller source
snapshots/callback allocations and SDK/allocator overhead have explicit boundaries.

Pin read identity before recovery, copy and source deletion. Every verified copy
compares full source/destination digests; native copy still incurs verification
reads. Move returns the confirmed destination even when deletion fails or times out.
Never convert an uncertain completion into a new upload. Operation metadata enables
provider-specific reconciliation but grants no authority and proves no content hash.

Join workers before abort. Bound the cleanup request separately, retain safe cleanup
debt, and preserve the original error/cause. Completion interruption remains unknown.
Best-effort final observers execute beyond the work deadline with their own bounded
timeout. Durable side effects and persistent restart are phase 5 responsibilities.

See [the public contract](../../docs/transfers.md) and
[local/resource evidence](../../docs/evidence/phase-4.md). Hosted S3 evidence remains
pending at the owner's request. Other native protocols and runtime qualification
retain their original phases.
