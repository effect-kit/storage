# 002 — Portable objects, identity, capabilities and cursors

Date: 2026-10-28. Decision: accepted; portable service implemented in phase 2.

## Decision

Storage represents one configured namespace and requires put, scoped get, head,
delete and listPage. Helpers add exists, explicitly bounded collection, lazy
listing and per-object bulk outcomes. Separate services provide signing, native
copy, multipart, resumable protocols, versions/governance and administration.
Applications declare the capabilities they need through ordinary Effect services.

ObjectKey is a nonempty, well-formed Unicode string without NUL. Core never collapses slashes, changes
Unicode normalization, decodes percent sequences or applies OS path resolution.
Adapters validate their accepted key/metadata/header domains; an opaque key is
neither authorization nor a safe filesystem path.

ObjectSize is a nonnegative safe integer number, serialized as a JSON number.
Reject values outside that domain before rounding; all initially targeted object
limits fit it. Native versions, generations and metagenerations remain strings.
The GCS SDK probe shows that even a string constructor option can round an unsafe
generation. Its adapter must guard that path or use lossless authenticated HTTP.
Never silently coerce a native reference through Number.

ETags are opaque validators. Checksums carry algorithm and full-object/composite
meaning separately. Read NotModified is a distinct result; ranges distinguish
returned bounds from complete object size. Metadata and body identify one response.

Missing get/head is an explicit expected failure. Exists catches only confirmed
missing. Missing delete is idempotent success for the specified target; version
markers and permanent version deletion retain native semantics. Create-only must
be a native atomic condition or fail unsupported. Copy/move preserve source
identity and partial outcomes; cross-store move is not atomic.

## Cursors and capability profiles

Version cursor envelopes and bind them to provider/store/query configuration,
including prefix/delimiter and relevant profile. Validate untrusted envelopes;
reject mismatches and unsupported versions. Listing is lazy, bounded by pages,
and not a snapshot under concurrent writes. Whether a native token survives
restart is explicit per provider.

Cursors grant no authority. HTTP APIs derive namespace/prefix server-side and
must integrity-protect or server-store continuation state when clients could
otherwise forge sensitive scope. A query fingerprint is not authentication.

Capabilities describe operations, native constraints, configuration and runtime.
GCS offsets, Azure blocks, S3 parts and tus offsets remain distinct low-level
protocols with shared higher-level workflows. TypeScript interfaces alone do not
complete a feature; dynamic selection needs a typed unsupported outcome.

The [portable API](../../packages/storage/src/storage.ts) specifies implemented conditions, reference
variants, codecs and helpers. Advanced protocol/lifecycle interfaces establish
boundaries; they do not imply implemented native capabilities.
