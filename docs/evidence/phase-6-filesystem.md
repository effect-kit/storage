# Phase 6 filesystem evidence

Date: 2026-10-30. Effect/platform 4.1.0, Darwin arm64; Node 24.22.0 and Bun 1.4.4.
The filesystem provider is implemented independently of native R2 Workers.

The offline suite covers twelve applicable portable conformance cases (one
match-current write case skipped by capability), opaque key preservation,
independent-instance create-only races, open-reader generation consistency,
source/length failures, unsupported conditions before source consumption, symlink
rejection, root identity/corrupt envelopes/cursor isolation and managed driver use.
Protected download sessions also consume this provider and publish verified file targets.
A separate Bun handle regression verifies positional rewrites and explicit reads
without changing the logical cursor.

`node scripts/filesystem/check.ts [node|bun]`, after `pnpm build`, runs the actual
engine across eight fresh-process SIGKILL boundaries: first body write, staged file
sync, before/after replacement rename, before/after object-directory sync, and
before/after create-only hard-link publication. Reopened objects retain complete
matching bytes/metadata. Explicit exclusive-owner reclamation removes staging debt.
Four independent writers produce one create-only winner and three condition failures.
Each engine also runs portable conformance and a generated 32 MiB streaming put.
Reports: `.artifacts/filesystem-node.json`, `.artifacts/filesystem-bun.json`.

The Bun suite initially reproduced incorrect header offsets. A native fs probe
confirmed ignored undefined write offsets/lengths and bigint read positions. The
runtime's focused handle adapter uses explicit numbers/lengths and the final real
Bun suite passes. This is a compatibility fix, not a claim about untested Bun versions.

`pnpm ci:check` verifies formatting, catalogs, lint, source/test/tooling types,
offline tests and builds. `pnpm test:extended` checks the exact archive, public
filesystem declarations in SDK-free NodeNext/Bundler consumers and package exports.

No hosted resources are used. Linux/Windows, hostile root replacement, permission/
disk exhaustion across real deployments, network filesystems, physical power-cut
and long soak remain separate qualification lanes. Process-kill success cannot
establish device-cache persistence. Match-current write/delete and native multipart
remain explicitly unsupported. See [filesystem contract](../filesystem.md).
