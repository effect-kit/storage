# 009 — Filesystem object envelopes

Status: implemented for the locally tested Darwin arm64 Node/Bun profiles.

Use a private, trusted application-owned root. Hash exact UTF-8 keys into fixed
internal filenames and retain the key in validated metadata. Keys never become
path segments. The storage format binds a stable storeId and root identity.

One immutable file contains a 64 KiB reserved metadata header and the streamed
object body. A producer owns its private staged file until content validation,
final metadata encoding, file sync and close complete. Rename publishes replacement;
no-replace hard-link publication supplies create-only. Sync the object directory
before acknowledging success. Open descriptors retain the same envelope across
replacement, avoiding independently published sidecars or mutable manifest races.

Use opaque fresh validators and recorded full-object SHA-256. No historic version,
match-current mutation, conditional deletion or multipart capability is invented.
A stream put does not collect its source. The primitive-only managed driver retains
the engine's existing bounded single-request semantics. Listing scans directory
arrays and envelopes under a documented explicit scale limit; it is not an index.

Ordinary interruption joins issued writes/publication then removes owned staging.
A killed process can leave staging debt. Reclamation requires explicit application
exclusion of all writers; no age heuristic proves an upload abandoned. Never delete
published objects while reclaiming staging.

Bun 1.4.4 ignores the undefined offset/length write overload and bigint positional
reads used by Effect 4.1's shared Node filesystem implementation. Native probes and
fresh-process tests reproduced corrupt header placement. The isolated Bun runtime
open adapter uses explicit numeric offset/length calls, scoped native handles and
the same FileSystem interface; other filesystem operations use the official layer.
No globals or dependency source are patched. Tests cover offset rewrites, cursor
preservation and real Bun crash recovery. Revisit this adapter with upstream fixes.

Hostile concurrent local writers, ancestor replacement, external in-place changes,
power loss, network filesystems and unexecuted OS profiles remain outside evidence.
No-follow checks are final-component defenses under the trusted-root assumption.
See [provider contract](../../docs/filesystem.md) and phase 6 evidence.
