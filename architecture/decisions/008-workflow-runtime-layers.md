# 008 — Workflow and runtime layers

Status: implemented before phase 6, on Effect 4.1.0.

Provider layers supply a TransferDriver companion over the advanced TransferEndpoint
contract. The driver owns provider limits, native protocols, reconciliation and optional
opaque multipart-handle encoding. S3 builds it from its existing client without extra
requests. Memory supplies only supported primitives. Storage remains usable without
managed workflows or persistence. Driver/Storage identity mismatch fails construction.

Transfers.make/layer accept execution policy only. Runtime layers supply TransferHash
and Effect SecureRandom; neither belongs in numeric policy. Transfers.layer also
provides a private managed-runtime companion. UploadSessions.make/layer capture it
and UploadSessionStore, so ordinary and durable operations share admission, request
limits and hash/policy choices. Separately constructed managed layers have separate
budgets. Durable attempt debt remains per-session and survives reconstruction.

The canonical runtime paths are `/node`, `/bun`, `/deno`, `/browser`. Selected Effect
platform services compose with our focused hash/protection helpers. Native Node/Bun
crypto stays isolated. Deno/browser use maintained incremental noble hashing and Effect
Aead; complete-array Digest is used only for bounded identities. Checkpoint protection
keys are explicit application configuration. Core and generic engine graphs do not
load runtime peers, provider SDKs or noble hashing.

File sources, atomic checkpoint backing and restartable targets share Effect
FileSystem/Path algorithms. They preserve bounded reads, full writes, private files,
file/directory sync, verified publication and joined mutation cancellation. Trusted
directory ownership remains an application obligation; this is not a confined object
provider or distributed lease. Hashed checkpoint filenames do not invent key inventory.

The package is unpublished: superseded runtime paths and endpoint-bearing constructors
are removed together from source, exports, consumers, examples and archive checks.
No forwarding aliases or old-development-checkpoint migration layer is maintained.
Workflow outcomes, source/callback E/R, scopes, uncertainty and explicit ownership remain.

The four runtime helper smoke lanes use exact archives, isolated peers/declarations and
actual engines. Qualification is specific: provider transports, OS crash/power-cut
behavior, browser reload/selection and distributed fencing require their own evidence.
See [runtime contracts](../../packages/storage/src/node.ts) and [runtime checks](../../scripts/runtime/check.ts).
