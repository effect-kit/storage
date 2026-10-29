---
"@effect-kit/storage": minor
---

Add protected, versioned durable upload/download sessions and batch checkpoints over
Effect KeyValueStore. Include explicit exclusive ownership, safe reconciliation,
stable at-least-once events, restartable download targets and optional Node atomic
filesystem/AES-GCM integrations. PostgreSQL and distributed leases are not required.
