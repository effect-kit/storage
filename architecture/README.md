# Storage architecture

Status: phase 1 foundation. Only ObjectKey/ObjectSize schemas are implemented.
These decisions guide the portable service and adapters in subsequent phases.

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
what the local probes actually establish.

Create modules/exports when their behavior exists. No production code, CI check or
required documentation input may depend on ignored research.
