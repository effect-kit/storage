# Storage architecture

Status: portable API, memory/conformance and phase 3 S3 implementation delivered;
hosted AWS acceptance remains pending.
These decisions guide native adapters and transfer workflows in subsequent phases.

| Decision                                                                        | Responsibility                                              |
| ------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| [001 — Boundaries and transports](decisions/001-boundaries-and-transports.md)   | Package direction, optional dependencies, SDK selection     |
| [002 — Object contracts](decisions/002-object-contracts.md)                     | Keys, sizes, identity, operations, cursors and capabilities |
| [003 — Streams and resources](decisions/003-streams-and-resources.md)           | Scope ownership, source types, client disposal, buffering   |
| [004 — Outcomes and retries](decisions/004-outcomes-and-retries.md)             | Failure classification, uncertainty and retry budgets       |
| [005 — Persistence and fixtures](decisions/005-persistence-and-fixtures.md)     | Session store, filesystem threat model, local services      |
| [006 — Qualification and releases](decisions/006-qualification-and-releases.md) | Evidence, runtimes, CI and artifact/release boundaries      |

Use the [catalogs](../docs/catalogs.md) to track delivery. Each feature has a target
and implementation phase; planned contracts and documented vendor support are not
completed implementations. [Phase 1 evidence](../docs/evidence/phase-1.md) records
what the local probes actually establish. [Phase 2 evidence](../docs/evidence/phase-2.md)
records core/memory behavior, types and packed custom-adapter checks.

Create modules/exports when their behavior exists. No production code, CI check or
required documentation input may depend on ignored research.

[Phase 3 evidence](../docs/evidence/phase-3.md) separates the implemented S3 adapter,
controlled HTTP results, owned RustFS acceptance and the open AWS hosted gate.
