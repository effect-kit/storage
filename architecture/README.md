# Storage architecture

Status: portable API, memory/conformance S3 and in-process managed transfers delivered;
durable sessions and runtime helper layers delivered. Hosted AWS acceptance remains pending.
These decisions guide native adapters and transfer workflows in subsequent phases.

| Decision                                                                        | Responsibility                                              |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| [001 — Boundaries and transports](decisions/001-boundaries-and-transports.md)   | Package direction, optional dependencies, SDK selection     |
| [002 — Object contracts](decisions/002-object-contracts.md)                     | Keys, sizes, identity, operations, cursors and capabilities |
| [003 — Streams and resources](decisions/003-streams-and-resources.md)           | Scope ownership, source types, client disposal, buffering   |
| [004 — Outcomes and retries](decisions/004-outcomes-and-retries.md)             | Failure classification, uncertainty and retry budgets       |
| [005 — Persistence and fixtures](decisions/005-persistence-and-fixtures.md)     | Session store, filesystem threat model, local services      |
| [006 — Qualification and releases](decisions/006-qualification-and-releases.md) | Evidence, runtimes, CI and artifact/release boundaries      |

Feature/provider delivery tracking stays in ignored research. Planned contracts
and documented vendor support are not completed implementations. [Artifact checks](../scripts/artifacts/check.ts) validate
packed consumers; [core conformance tests](../packages/storage/test/conformance.test.ts)
exercise the portable memory contract.

Create modules/exports when their behavior exists. No production code, CI check or
required documentation input may depend on ignored research.

[S3 acceptance tests](../packages/storage/test/provider/s3.acceptance.ts) run against
the owned RustFS fixture or explicitly configured AWS resources. Hosted AWS
qualification remains pending.

[007 — Managed transfer ownership](decisions/007-managed-transfers.md) records shared
admission, protocol attempts, verified copy/move, uncertainty and observer boundaries.

Phase 5 adds [Effect-backed durable sessions](../packages/storage/src/sessions.ts) and
[separate-process recovery tests](../packages/storage/test/provider/sessions.acceptance.ts). ADR 005 now records
the owner-approved exclusive-ownership base; PostgreSQL/distributed leases are optional
backend work rather than required infrastructure.

[008 — Workflow and runtime layers](decisions/008-workflow-runtime-layers.md) records
provider-owned drivers, policy-only constructors, shared managed/session admission
and the canonical four runtime paths. [Runtime checks](../scripts/runtime/check.ts)
exercise the helpers separately from provider/OS/release qualification.

[009 — Filesystem envelopes](decisions/009-filesystem-envelope.md) records atomic
single-generation publication, opaque key mapping, explicit staging ownership and
the qualified Bun positional IO adaptation.

[010 — Native R2 binding](decisions/010-r2-worker-binding.md) records native result
mapping, stream ownership, multipart completion limits and the explicit boundary
between local workerd acceptance and hosted qualification.
