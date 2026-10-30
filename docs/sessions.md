# Durable sessions with Effect persistence

`@effect-kit/storage/sessions` stores protected, schema-versioned checkpoints using
Effect 4.1's `KeyValueStore.toSchemaStore`. Applications supply the KeyValueStore
layer. There is no PostgreSQL, Redis, ORM or hosted coordinator requirement.
The canonical `/node`, `/bun` and `/deno` modules compose runtime crypto, atomic
filesystem persistence and restartable file targets. `/browser` supplies portable
hashing/protection; applications supply its backing store and target. See
[runtime setup and support limits](runtimes.md).

## Ownership and backing guarantees

The base profile requires **one application owner of a namespace across processes**.
Pass `ownership: "exclusive"` only when the application or supervisor ensures this.
An in-process semaphore serializes mutating callers sharing the same KVS service object;
independently constructed clients pointing at the same database/directory do not
magically share that semaphore. Durable uploads currently send parts sequentially;
planner reservations remain conservative when partConcurrency is larger than one.

A custom `SessionOwnership.withExclusive` may enforce stronger application ownership.
It must exclude competing owners for the entire effect, including final checkpoint
writes. This is not an expiring-lease API. Generic KVS get/set/modify cannot provide
cross-process CAS or stale-worker fencing. Distributed leases need a backend with
fenced writes and separate qualification; no built-in distributed backend is shipped.

Any Effect KVS can be supplied, but its guarantees remain its own. Memory is useful
for tests, not restart durability. Effect's generic filesystem KVS does not establish
atomic checkpoint replacement by itself. `atomicFileStore(directory)` uses private
0600 temporary files, file fsync, atomic rename and directory fsync; it also syncs
newly created ancestor entries. Its root must be trusted and exclusively owned.
It fails when the platform cannot support those operations, rather than claiming
durability. Local Darwin process tests are evidence; power-cut/OS/network-filesystem
qualification is not implied. An abrupt kill can leave encrypted `.pending-*` files;
readers ignore them. Remove abandoned temporaries only after proving exclusive ownership.

The atomic checkpoint backing cannot reconstruct logical keys from its hashed
filenames. Its `keys` operation fails with a typed KeyValueStoreError; nonempty
`clearPrefix` calls fail without removing records. Use application-known session IDs
for recovery. Other application-supplied KeyValueStores may support enumeration.

## Configure a session service

```ts
import { Effect, Layer } from "effect";

import * as StorageNode from "@effect-kit/storage/node";
import { S3Storage } from "@effect-kit/storage/s3";
import { UploadSessions } from "@effect-kit/storage/sessions";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";

const ManagedLive = Transfers.layer({
  maxBytes: 1024 * 1024 * 1024,
  memoryBudgetBytes: 64 * 1024 * 1024,
}).pipe(Layer.provideMerge(S3Storage.layer(config.s3)));

const StoreLive = StorageNode.sessionStore({
  directory: config.checkpointDirectory,
  namespace: config.sessionNamespace,
  key: config.checkpointKey,
  ownership: "exclusive",
});

const AppLive = UploadSessions.layer({
  configurationId: config.storageConfigurationId,
  expiresInMillis: 24 * 60 * 60 * 1000,
}).pipe(
  Layer.provide(StoreLive),
  Layer.provideMerge(ManagedLive),
  Layer.provideMerge(StorageNode.layer),
);

const resume = Effect.gen(function* () {
  const sessions = yield* UploadSessions;
  const source = yield* UploadSource.file(config.uploadPath);
  return yield* sessions.resume(config.uploadSessionId, source);
});

await Effect.runPromise(resume.pipe(Effect.provide(AppLive)));
```

`config` is application configuration, including a stable Redacted 32-byte key.
Create the session once with `sessions.create({ id, key, source })`, then resume
with the same identity/source after restart. The [runnable example](../packages/storage/examples/durable-upload.mjs)
shows complete environment-based setup.

The managed layer supplies a private companion so ordinary and durable workflows
share one admission budget, request semaphore, hash implementation and policy.
`UploadSessions.make(options)` captures that companion plus UploadSessionStore;
use a context built from `Transfers.layer`, rather than a standalone Transfers.make
result. The provider owns native handle encoding. Unsupported durable upload
protocols/codecs fail before opening the source or starting an upload.

For existing persistence, use `UploadSessionStore.layer({ namespace, ownership,
protection })` with your Effect KeyValueStore layer. File conveniences construct
that same service. They do not install a scheduler or distributed coordinator.

The key must survive restart through application secret configuration. Generating a
new key on every startup makes old records unreadable. The protection interface
also accepts an application codec backed by a secret service/key rotation scheme.
The bundled helper uses authenticated AES-256-GCM with namespace/session associated
data. Keep the key outside the checkpoint directory. Entire records are protected,
including target keys, fingerprints, native handles, metadata and events. Runtime
handles are Redacted; public status never returns them. Never log an explicitly
encoded/decrypted checkpoint. Credentials, clients and callbacks are never serialized.

Bind `configurationId` to provider/authority/namespace/encryption policy identity
without putting credentials into it. The engine also checks namespace, provider
and storeId. Credential refresh for the same authority is allowed; changing
principal/tenant or encryption policy requires deliberate application validation.
Session IDs are correlation identifiers, not bearer authorization. The backend
application must authorize access; use separate namespaces/protection for tenants.

## Upload creation and recovery

- `create({ id?, key, source, headers?, condition? })` fingerprints a replayable,
  known-length source and persists a created record. Supply a unique stable client
  ID if the caller must recover a lost create response. Existing IDs conflict.
  Do not reuse IDs, even after forgetting old records.
- `resume(id, source)` / `complete(id, source)` run or continue the multipart upload
  and verify the returned destination version by full checksum readback. Completed
  sessions return their recorded receipt without a second completion.
- `status(id)` reports state/revision, expiry, completed part count and final metadata.
  Read-only polling can observe committed checkpoints during an active transfer;
  a non-atomic custom backing must fail closed on a partial read rather than serve it.
- `reconcile(id)` checks completion proof and persists verifying or completion-unknown.
- `abort(id)` records abort intent, removes only its observed native upload handle,
  and retains cleanup-pending on failure. Interrupted/lost aborts are resolved using
  inventory evidence on retry. It never deletes a completed destination object.

The initial upload strategy is numbered multipart, including small objects. One-shot
or unknown-length sources cannot promise restart recovery; ordinary Transfers still
supports them. Recreate the same bytes/file/replay factory after restart. The engine
checks the full source digest before work, checks it again on the actual read, and
stores per-part source digests. Recovery skips a saved part only when provider
identity/size and current source bytes agree. Lost uncheckpointed parts can be
re-uploaded to the same numbered handle with stable bytes. Source scanning still
reads old prefixes; avoiding network re-upload does not eliminate local/source IO.
The S3 driver encodes native checksum policy with the upload handle for restart.

Intent is saved before initiation, part calls and completion; acknowledgments are
saved after provider responses. Upload attempt intents, including verification,
consume a durable lifetime budget across resume calls. A crash before sending can
still consume an intent. Per-invocation deadlines and shared byte/request admission
also apply. Reconciliation/abort have separately bounded control requests so an
exhausted transfer budget does not prevent inspection/cleanup. Raising a depleted
budget is an explicit application policy decision. Storage/protection callbacks must
be bounded and honor their contract. Checkpoint commits and mutating file-target IO
are joined before ownership is released; cancellation can wait for an issued local
filesystem operation to finish.

A crash around initiation with no saved upload ID remains initiating/orphan debt.
Key equality alone cannot prove which upload to adopt or remove. Completion intent
with no receipt is not blindly repeated: exact operation nonce/size proof may move
it to verification; otherwise completion-unknown remains. This is intentional
uncertainty, not an automatic restart guarantee at every boundary. Native provider
inventory/reconciliation limits remain visible. Rejected completion can return to
uploading; uncertain completion cannot. Independent operation nonces prevent
cross-namespace/client-ID confusion. Expiry blocks new data work but permits settling
an already committed/verifying result and explicit cleanup.

## Download recovery and file publication

`createDownload({ id, ref, expectedChecksum? })` captures a sized, pinned source.
`resumeDownload(id, target)` requires the same owned restartable target; Node's
`yield* StorageNode.fileDownloadTarget({ id, path })` captures runtime services and binds its identity to canonical destination and
private partial paths. Use a unique id per session. Generic targets promise a stable
identity, exact prefix reads, positional writes, sync, and idempotent publication.
An optional discard method supports `abortDownload(id, target)`.

Each chunk is synced before its offset/prefix digest is checkpointed. On restart,
uncheckpointed tail bytes are discarded and the saved prefix is rehashed. Ranges
pin the same native version or strong validator; changed source, prefix or target
fails. Full expected checksums are compared when supplied/available. Digest snapshot
support is required (all four runtime helpers provide it); internal hash
state is never serialized. A crash at the last byte before EOF validation causes
a safe full re-read rather than treating a byte count as proof of completion.

Verified state is committed before publication. The shared file target hashes the partial,
renames it and syncs the directory. If the response/checkpoint is lost after rename,
it recognizes an existing final file only by exact size/digest. Existing destinations
are replaced by successful publication; authorize paths and exclusive partial access
in the application. Unrelated files never authorize cleanup. Abort discards only the
owned partial and cannot undo verified/published work. Browser reselection and other
OS/filesystem guarantees are not automatically inferred from this interface.

## Durable events, batches and retention

`deliverEvents(id, deliver)` drains upload completion/abort events. Delivery is
**at least once**: a crash after the receiver commits but before acknowledgment
replays the same stable event ID. The receiver must deduplicate transactionally.
Callback E/R are preserved; a failure/timeout leaves delivery pending without
changing the successful transfer. No application database/cloud transaction is
claimed. Callbacks must not recursively acquire the same session store owner.

`createBatch(sessionIds)` saves a bounded manifest of existing independent sessions.
`processBatch(id, run)` processes entries sequentially, retains completed peers and
records failed/unknown outcomes. Restart inspects each child's durable state before
running it again. Application callbacks provide sources/targets, dependencies and
reconciliation policy; a status record alone cannot reopen an arbitrary stream.
Keep session/manifest IDs in your application registry; these recovery APIs use
known IDs and do not depend on backing-store key enumeration. There is no hidden daemon or automatically installed queue.

`forget(id)` removes only terminal records after required upload events are delivered.
Expiry does not erase cleanup debt or pending events. Applications choose retention
and explicitly inspect/reconcile/abort/drain known IDs. The engine has no governance
bypass and does not remove unobserved uploads or arbitrary quarantine objects.

## Checks and example

Run `pnpm ci:check` and `pnpm test:extended` for offline/type/packed checks.
`pnpm test:s3:local` builds the package and runs owned RustFS plus separate-process
restart tests. No AWS account is needed. See [phase 5 evidence](evidence/phase-5.md) and the current
[API/runtime regression evidence](evidence/pre-phase-6.md).

The repository example is `packages/storage/examples/durable-upload.mjs`. After
`pnpm build`, configure S3/optional local endpoint, a private checkpoint directory,
a stable base64 checkpoint key and configuration identity through the environment.
Use the same unique ID, input bytes and configuration across these commands:

```sh
node packages/storage/examples/durable-upload.mjs create UNIQUE_ID ./video.bin uploads/video.bin
node packages/storage/examples/durable-upload.mjs resume UNIQUE_ID ./video.bin
# Interrupt the resume process, then run the same resume command in a fresh process.
node packages/storage/examples/durable-upload.mjs status UNIQUE_ID
node packages/storage/examples/durable-upload.mjs reconcile UNIQUE_ID
```

Environment: S3_BUCKET, S3_REGION, optional S3_ENDPOINT, STORAGE_CHECKPOINT_KEY,
STORAGE_CHECKPOINT_DIRECTORY and STORAGE_CONFIGURATION_ID. Normal SDK credential
configuration stays outside checkpoints. Use an authorized bucket/prefix; hosted
AWS acceptance remains postponed at the owner's request.

## Additional provider profiles

Filesystem storage provides pinned conditional reads for download sessions and no
native multipart upload protocol. Native R2 Workers also supports the download
session contract, but its missing inventory/conditional multipart completion means
no durable upload handle codec is supplied. Upload creation fails before source
reads. Neither a primitive driver nor local handle reconstruction implies restartable
multipart upload support. See the [filesystem](filesystem.md) and [R2](r2-workers.md)
provider guides for exact capabilities and runtime evidence.

GCS and Azure now supply durable native upload strategies through the same layer
composition. GCS checkpoints protect the resumable bearer URL and probe the actual
committed offset on restart. Azure checkpoints retain deterministic block IDs,
source checksums and sizes; incomplete or uncertain stages are safely repeated.
Both fingerprint the complete replayable source and verify the pinned publication.
No S3 part façade or provider registry is used. Offset handles and named-block
records have separate checkpoint fields; checkpoints contain no cloud credentials.

Azure abort abandons only this session's block IDs. It never deletes the committed
blob and cannot promise immediate uncommitted-block reclamation. GCS abort cancels
the known native session; unknown initiation still requires orphan discovery.
`pnpm test:native:local` verifies nine SIGKILL/restart boundaries using the production
protected file store. See [phase 7 evidence](evidence/phase-7.md) for exact limits.

Supabase tus reuses offset checkpoints through `/supabase`. Completion metadata is
resolved in a separately budgeted request and verification retains the commit
receipt's validator. Reconciliation cannot adopt another writer's object merely
because its bytes match. A finished URL that has disappeared can be reconciled
only through the saved operation marker and size, followed by pinned verification.
`pnpm test:app:local` covers five actual tus SIGKILL/restart boundaries.

Vercel does not supply an upload handle codec: its public SDK lacks the inventory
needed for safe crash recovery. Durable upload admission fails before initiation;
in-process multipart cancellation reports remote cleanup limitations. See the
[Supabase](supabase.md) and [Vercel](vercel.md) guides for exact profiles.
