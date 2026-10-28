# 004 — Expected failures, mutation outcomes and retries

Date: 2026-10-28. Decision: accepted; production implementation follows in phases 2–5.

## Decision

Use a tagged StorageError with operation, provider, sanitized reason/cause,
safe request correlation and mutation outcome. Expected reasons include invalid
input/configuration, unsupported, missing/denied, precondition/range failures,
expiry, throttling, transport, integrity, session and partial-transfer errors.
Caller source failures, defects and interruption retain their own meaning.

Mutations distinguish not-started/authoritatively rejected, confirmed success,
unknown and partial outcomes. A timeout or lost completion response can follow a
commit. Later missing upload state or a new precondition failure alone does not
prove the earlier mutation failed.

Reconcile unknown outcomes using pinned identities, native part/offset/version
state, checksums and durable operation intent. Do not promise exactly-once
external writes or undo on cancellation. Cleanup failure preserves the original
failure and creates visible bounded cleanup debt.

## Retry ownership

Direct adapter calls start with bounded transport attempts and explicit
operation/body-aware policy. Managed transfers own one total deadline and
attempt budget across parts, transport libraries and Effect schedules. Disable
or account for nested native retries; the phase 1 probes use one S3/Azure attempt
and disable GCS automatic retries.

Permanent authorization/configuration failures are not retried. Part replay
requires retained bounded bytes or a verified replayable source. Unknown
completion is reconciled before retrying. Respect supported Retry-After and
bounded jitter/backoff without exceeding the overall deadline.

## Consequences

Protocol-specific native errors map at the adapter boundary; HTTP success may
still carry an operation failure. Per-object bulk results retain independent
outcomes. Observer errors after confirmed commit cannot relabel it as rejection.
Required downstream delivery uses durable state/outbox integration, separately
from best-effort progress.
