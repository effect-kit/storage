---
"@effect-kit/storage": minor
---

Simplify managed workflows with provider-supplied transfer drivers, policy-only
Transfers construction and sessions that share the configured runtime budgets.

Add canonical Node, Bun, Deno and browser runtime modules, portable incremental
hashing/protection and shared filesystem checkpoint/download helpers. File sources
capture their hash dependency; restartable file target construction is effectful.
Remove unpublished `/transfers/node`, `/sessions/node`, `/s3/transfers` paths and
manual endpoint/digest/handle constructor arguments. See the runtime/session guides
for layer composition, explicit persistence ownership and qualification limits.
