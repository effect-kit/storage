# Phase 4 evidence — Managed transfers

Executed on 2026-10-29 with Node 24.22.0, Effect/@effect/vitest 4.0.6,
TypeScript 7.0.2 and official AWS SDK 3.1159.0. This is in-process/local protocol
evidence. No AWS account was used; the owner-postponed hosted gate remains open.

## Behavioral and artifact checks

- `pnpm format` and `pnpm ci:check`: formatting, 47-feature/16-provider catalog
  validation, lint, source/test/tooling types, 10 tooling tests, 97 package tests
  and tsdown ESM/declaration builds.
- `pnpm test:extended`: strict Publint/ATTW ESM, exact packed file/export policy,
  isolated SDK-free core/testing/transfers runtime, NodeNext/Bundler declarations,
  source/sink E/R failures, Node digest, client-only S3 transfer hooks and explicit
  signing/POST peers. Core/transfers require no provider SDK or Node typings.
- `packages/storage/test/transfers/`: immutable sources, zero/unknown lengths,
  bounded backing allocations, replay protection, retained-part retry, one shared
  attempt budget, Retry-After, deadline/abort bounds, worker joining, FIFO admission,
  uneven producers/parts, cancellation, source/target failures, lost completion,
  pinned range recovery/replacement, full checksum/length mismatch, verified
  cross-store copy, partial move, ordered lazy batches, file fingerprint/staging,
  and bounded/failing/hanging observers.
- The controlled HTTP suite uses the real SDK for a 503 part throttle and lost
  completion response. Six observed HTTP requests equal six engine attempts;
  completion reconciles once without another MPU or abort. SDK retries remain one.

## Owned local S3 and resource run

`pnpm test:s3:local` starts a temporary versioned RustFS bucket on a loopback-only
port with generated credentials, pinned image, 1 GiB container limit and tmpfs.
Its existing cleanup removes owned versions/uploads, the container and credential
file. Eight tests pass; one AWS-only authority/region test is explicitly skipped.
The portable acceptance test includes all 13 public conformance cases.

The managed suite verifies unknown empty MPU, native copy, forced multipart copy,
full destination readback and source-version deletion. Its resource case generates
320 MiB from fresh 64 KiB chunks, uploads 64 five-MiB parts with two workers, then
downloads to a discard sink and compares full SHA-256. The 32 MiB engine budget
is ten times smaller than the object; no full-object source buffer is created.

The configured source/transport/digest/receipt reservation peaks at **18.53125 MiB**
(19,431,424 bytes), including 512 KiB of bounded part receipts. Download has a
separate smaller reservation. Active protocol requests peak at two and finish at
zero; reservations finish at zero. There are 68 total work attempts: create,
64 parts, complete, HEAD and GET. Native SDK checksum work also runs.

Measured process memory and active-resource counts are recorded in the ignored
`.artifacts/transfers-resources.json`. Sampling is every 20 ms plus progress events.
The manual provider config exposes Node GC; the resource case collects before its
baseline and after progress callbacks, additionally recording retained samples.
This is a resource profile, not a throughput benchmark. The preceding copy test
shares the Vitest process; allocator/GC and sample timing still vary between runs.

| Measurement       |    Baseline | Observed peak | End of transfer |
| ----------------- | ----------: | ------------: | --------------: |
| RSS bytes         | 201,801,728 |   212,336,640 |     194,199,552 |
| Heap used bytes   |  35,585,776 |    37,696,856 |      34,142,680 |
| External bytes    |  29,669,750 |    34,235,460 |       4,410,744 |
| ArrayBuffer bytes |   4,858,854 |    30,208,251 |         389,443 |
| Active resources  |           4 |            18 |               4 |

The peak GC-assisted retained ArrayBuffer sample is 10,874,946 bytes,
versus a 4,858,854-byte baseline. This retained binary-memory delta is
asserted within the 32 MiB budget for this controlled fixture. The profile also
asserts that ordinary sampled incremental ArrayBuffers stay below a whole 320 MiB
object. Sampling is not a proof about every instantaneous allocation or SDK path.

A preceding run without forced GC measured RSS from 200,966,144 to 251,772,928
bytes (about 48.45 MiB growth), exceeding logical reservations. S3's configured
transport allowance is 1 MiB per concurrent request; Node digest state is budgeted
at 32 KiB. The GC-assisted samples support bounded live binary retention for this
SDK/fixture, while total RSS includes the runtime, allocator and collectible buffers.
Neither these allowances nor the test establish an RSS ceiling, cloud throughput
or hosted billing behavior. Resource counts are sampled before the enclosing
client/fixture scope closes; scoped workers and request counters are separately
asserted closed. The container is a separate process.

## Qualification boundaries

- File behavior is tested through the Effect FileSystem contract. Confined provider
  behavior, real OS matrices and atomic-rename qualification remain phases 6/14.
- Expected digests verify transferred representation/length; a freshly calculated
  digest alone is not independent proof of authenticity. ETags and composite
  checksums are never substituted for a full-object digest.
- Native copy is fully verified by reading source and destination. These reads
  have real production cost; local speed is not a cloud performance claim.
- Durable sessions, persistent batch checkpoints, required audit effects and
  process-restart recovery remain phase 5. Download recovery/bulk feature families
  stay in progress in the catalog until that durable work exists.
- GCS offset, Azure blocks, Workers and tus/native provider strategies retain
  phases 6–9. Hosted S3, other runtimes/providers and adversarial long soak retain
  their existing gates. No new deferral or release/publish action was introduced.
