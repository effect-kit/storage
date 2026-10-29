---
"@effect-kit/storage": minor
---

Add the filesystem Storage provider with streamed single-file envelopes, atomic
create-only/replacement publication, pinned descriptor reads, ranges, listing and
explicit owned staging reclamation. Layers supply a primitive transfer driver.

Fix Bun positional file IO through an isolated scoped handle adapter after actual
runtime tests exposed ignored offsets in the upstream compatibility path. Preserve
explicit ownership, unsupported conditional mutation and OS qualification boundaries.
