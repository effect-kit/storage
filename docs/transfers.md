# Managed transfers

`@effect-kit/storage/transfers` owns upload/download/copy/move orchestration and
lazy batches. It depends only on Effect. `/transfers/node` supplies streaming
SHA-256 through Node crypto; `/s3/transfers` connects an existing S3Storage service
to the engine. No new dependency or managed-upload SDK is used.

```ts
import { Effect } from "effect";

import { S3Storage } from "@effect-kit/storage/s3";
import { s3TransferEndpoint } from "@effect-kit/storage/s3/transfers";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";
import { sha256 } from "@effect-kit/storage/transfers/node";

const program = Effect.scoped(
  Effect.gen(function* () {
    const storage = yield* S3Storage.make({ bucket: "application-bucket", region: "us-east-1" });
    const endpoint = s3TransferEndpoint(storage);
    const transfers = yield* Transfers.make(endpoint, {
      digest: sha256,
      memoryBudgetBytes: 64 * 1024 * 1024,
      maxBytes: 1024 * 1024 * 1024,
      partBytes: 8 * 1024 * 1024,
      partConcurrency: 3,
      requestConcurrency: 6,
      deadlineMillis: 15 * 60 * 1000,
      maxAttempts: 1000,
    });

    const uploaded = yield* transfers.upload({
      key: "reports/example.txt",
      source: UploadSource.bytes(new TextEncoder().encode("hello")),
      headers: { contentType: "text/plain" },
      condition: { _tag: "CreateOnly" },
    });
    yield* transfers.download({
      ref: uploaded.metadata.ref,
      expectedChecksum: uploaded.checksum,
      target: { write: (bytes) => Effect.sync(() => console.log(bytes.length)) },
    });
  }),
);
```

`Transfers.layer(endpoint, policy)` exposes the same service for static
`Transfers.upload/download/copy/move/uploadMany/downloadMany` calls. Share one
service instance to share its byte and request limits. Provider credentials and
owned clients retain the lifetime of their supplying layer/scope.

## Sources and planning

- `UploadSource.bytes(bytes)` takes an immutable defensive snapshot, including
  Buffer inputs. Source construction/snapshots and caller-retained input arrays
  are outside the engine's execution budget. The snapshot yields copied 64 KiB slices.
- `UploadSource.stream(body, length?)` opens once. It preserves the source E/R,
  defects and interruption. Reusing the consumed descriptor fails with BodyConsumed.
- `UploadSource.replayable(() => body, length?)` opens an equivalent representation
  on demand. The caller owns that equivalence guarantee. The engine currently
  retries retained request bytes, rather than automatically restarting sources.
- `yield* UploadSource.file(path, sha256)` requires Effect FileSystem at construction
  and captures it. It checks size/device/inode/mtime and a full incremental content
  fingerprint before each opening and at EOF. Same-size edits with unchanged
  timestamps fail. Preparation performs a bounded-memory full read; each opening
  fingerprints again. Reads use scoped 64 KiB handles without stream prefetch.
  Preparation is a separate caller effect; give it a timeout where appropriate.

Known small objects use a staged single request. Larger/unknown objects use the
endpoint's numbered multipart protocol when available. `strategy` can force single
or multipart. A single request stages its entire bounded body before publication;
it fails planning if it cannot fit. Unknown sources require a finite `maxBytes`
(default 1 GiB, capped by the endpoint). Multipart part size grows up front to fit
that maximum within the provider's part count. It never adapts by consuming the
last available part and hoping the source ends. Known impossible size, memory,
part-count or minimum attempt budgets fail before initiation. Zero bytes, exact
part boundaries and false declared lengths are checked from actual reads.

The accepted upstream **backing allocation**, not just the subarray's length, is
limited by `maxSourceChunkBytes` (default 1 MiB). Arbitrary upstream buffering,
source-factory allocations and consumer-retained output buffers are caller-owned;
the engine cannot bound allocations inside application callbacks.

## Shared resources

Admission is FIFO and reserves an entire transfer workspace before opening the
source. This avoids deadlocks between producers holding partial buffers and
workers waiting for memory. A large queued transfer cannot be repeatedly bypassed
by small transfers. Cancellation removes queued admissions; the total deadline
also bounds admission waits. The tradeoff is conservative admission and possible
head-of-line waiting.

Upload reservations include `(workers + 1) * partBytes`, an upstream chunk,
per-request transport allowances, digest state, declared source workspace, and
8 KiB per possible retained part receipt/completion serialization. Part identities
are bounded and validated before retention. Single requests reserve two body
buffers conservatively. File sources add 64 KiB plus their fingerprint digest state.
Download reservations cover two input chunks, digest state and a transport allowance.
Streamed copy reserves source and destination workspaces together before either
starts; it needs at least two request slots. Multipart server copy reserves control
receipts/transport/digest state without pretending to allocate part payloads.

`transfers.resources` reports current/peak reservations and active protocol requests.
HTTP body lifetimes occupy request slots, including slow sink writes. Multipart
workers retain their own buffers across backoff. Native SDK/runtime/allocator
overhead is estimated separately: S3 declares 1 MiB per concurrent request and Node
SHA-256 declares 32 KiB of workspace. These reservations are **not an RSS ceiling**.
See the [measured local evidence](evidence/phase-4.md).

## Retries, cleanup and uncertain outcomes

One logical operation shares its deadline and `maxAttempts` across protocol calls,
HEADs, retries, copy stages and verification. S3Storage fixes the official SDK at
one attempt. Retry only transient transport failures, throttles and selected
transient service errors. Backoff uses capped exponential jitter, honors
Retry-After seconds/dates, and does not retry earlier if the server asks for a
delay beyond the remaining deadline. Permanent errors and source/sink failures
are preserved. A sink must acknowledge an entire chunk before continuation advances.

Numbered parts retry stable retained bytes using the same handle/number. Ambiguous
initiate, single PUT, native copy and completion are never blindly replayed. The
engine reserves `effect-storage-operation` in custom metadata; caller collisions
fail validation. S3 reconciliation requires an exact operation marker and size.
If proof is absent, the outcome remains unknown; it does not start another upload.
The marker is an operation identity, not a digest, secret or authorization boundary.

Failure/cancellation joins workers before aborting. Cleanup has one separately
bounded best-effort abort allowance (default five seconds), uses the shared request
semaphore, and cannot erase the original source failure/cause. This cleanup request
is outside the work attempt budget so exhausting that budget still allows cleanup.
Interruption during completion leaves uncertainty and a redacted handle for
reconciliation instead of racing an abort against a possibly committed object.

`transfers.diagnostics` retains at most 128 observer/cleanup/unknown-outcome records
and a dropped count. Explicit inspection may include the destination ref and a
redacted upload ID for cleanup. Nothing automatically logs these records. Required
cleanup ledgers, audit effects and restart checkpoints belong to phase 5.

## Download identity and integrity

Downloads select HEAD metadata and pin a native version/generation, or a strong
supported If-Match validator. On transient mid-body failure they reopen at the
acknowledged offset only with a pinned identity, known total and range support.
Changed versions, changed validators, wrong range bounds, short/long bodies and
checksum mismatches fail; a new representation is never appended to old output.
An unpinnable download can run once, but cannot resume after emitting bytes.

Every receipt has a calculated full-object digest. Uploads check caller-provided
expected digests before publication; S3 additionally uses native request/response
checksum validation. `bytesVerified` for an ordinary upload stays zero because
that receipt does not claim a destination readback. Downloads compare an explicit
expected digest or a matching full-object metadata digest, supporting hex/base64.
Their `bytesVerified` counts the successfully validated representation/length;
when no expected digest exists the calculated digest is not independent proof of
remote content authenticity. ETags are validators, never content digests. Composite
checksums cannot be substituted for full-object checksums.

These operations transfer the stored byte representation. Providers that transform
encoding must implement a separate representation contract before using these
hooks. Declared content type is metadata; no MIME sniffing is implied.

`Transfers.downloadFile({ key | ref, path, expectedChecksum? })` requires Transfers,
Effect FileSystem and Path. It writes a scoped temporary file beside the destination,
closes the handle, verifies the download, then renames. Failed downloads leave an
existing destination untouched. Successful rename replaces an existing destination
according to the supplied filesystem's semantics. Applications authorize paths;
this is not the confined filesystem provider planned for phase 6 or durable resume.

## Copy, move and batches

`copy({ source: sourceEndpoint, ref, destinationKey, condition? })` pins its source,
prefers native single/part copy where accepted, and otherwise bridges a download
and upload with a zero-capacity queue and backpressure. It verifies the destination
version against the full source digest. Native copy therefore saves client upload
traffic but still reads source and destination for full verification; account for
those reads when choosing a policy/provider. Verification failure after commit has
`mutationOutcome: "succeeded"`; a failed verification does not mean no object exists.

`move` is explicitly non-atomic. It deletes only a native pinned version or with a
strong supported delete condition, after confirmed verified copy. It returns Moved
or Copied with the destination receipt and delete error. A delete timeout also
returns Copied with an unknown delete outcome. Replacement never authorizes an
unconditional deletion of the new current object.

`uploadMany(stream, concurrency?)` / `downloadMany` return lazy ordered streams of
Effect Result values. Enumeration, workers and completed results are bounded by
concurrency; one failed entry does not discard others. Cancellation stops future
enumeration and joins workers. In-flight remote effects can still be uncertain;
these result streams are not persistent batch checkpoints.

Progress distinguishes bytes read, attempted sends (including retries), acknowledged
bytes, verified bytes, unknown totals and attempt counts. Server-side copy reports
zero client-sent payload bytes. Final observer notification runs after the operation
deadline boundary, with its own timeout, so a failed/hanging observer cannot reject
a confirmed result. Observers are best effort and have no additional environment;
capture dependencies when constructing the service. Effect spans and constant-label
metrics report operations, attempts, retries, reserved bytes and cleanup debt. Keys,
bodies, URLs and arbitrary request IDs are never metric labels or span attributes.

Run `pnpm ci:check`, `pnpm test:extended` and manually `pnpm test:s3:local`.
Hosted AWS, deployed Workers, other providers/runtimes, persistent recovery and long
soak remain separate qualification work.
