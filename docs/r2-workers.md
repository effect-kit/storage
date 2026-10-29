# Native R2 Workers storage

`@effect-kit/storage/r2-workers` wraps a caller-owned R2Bucket binding. It acquires
no SDK client, S3 credentials or account API authority. Configure the bucket in your
Worker application, then provide the layer at the request's runtime boundary.

```ts
import { Effect } from "effect";

import { Storage } from "@effect-kit/storage";
import { R2WorkersStorage } from "@effect-kit/storage/r2-workers";

const ProviderLive = R2WorkersStorage.layer({
  bucket: env.BUCKET,
  storeId: config.storageIdentity,
});

const put = Effect.gen(function* () {
  const storage = yield* Storage;
  return yield* storage.put({
    key: "documents/hello.txt",
    body: new TextEncoder().encode("hello"),
    contentType: "text/plain",
    condition: { _tag: "CreateOnly" },
  });
});
await Effect.runPromise(put.pipe(Effect.provide(ProviderLive)));
```

The application supplies a stable storeId identifying this bucket/authority. It
owns request authorization, binding configuration and Worker lifetime. The public
structural binding contract avoids a Cloudflare runtime/type peer. Compatibility
with the pinned official workers-types is checked in development; consumers need
only their own generated environment types. Stream body parameter declarations are
interpreted at one native boundary because lib.dom and Workers disagree on reader
overloads. Native return/ownership surfaces remain typed without Node declarations.

## Operations and identity

Put/get/head/delete/listPage use the same Storage contract. Conditions, metadata,
ETags, bounded/from/suffix ranges, prefix/delimiter listing and cursor pagination are
mapped at the binding boundary. Object reads return metadata from the body-bearing
response; no later HEAD is substituted. Missing objects map to NotFound, metadata-only
conditional reads to NotModified or PreconditionFailed as appropriate, and null
conditional writes to rejected PreconditionFailed.

Portable validators retain HTTP quotes. Structured native conditions receive the
unquoted ETag required by R2. ETags remain opaque validators. The binding's version
field does not establish retrievable history; this adapter advertises no versioned
reads. Downloads pin using supported IfMatch instead.

Native checksum observations retain their algorithm and full-object meaning. ETags
are never converted into checksums. Writes returning a confirmed receipt but invalid
metadata fail with mutationOutcome succeeded; an unreadable receipt does not undo
publication. Known native error codes become safe reasons, including throttling,
authority failures and uncertain disconnects; raw errors/messages never enter
StorageError. Unknown failures remain conservative ProviderFailure/unknown mutations.

Cloudflare documents the [binding result unions](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/),
[native limits](https://developers.cloudflare.com/r2/platform/limits/) and
[error codes](https://developers.cloudflare.com/r2/api/error-codes/). The adapter
uses these contracts; local workerd results do not qualify hosted service behavior.

## Streams and limits

Byte-array puts are passed directly. A known-length Effect stream is pumped with
backpressure into a native FixedLengthStream; source E/R and length failures remain
visible. A runtime without that native constructor rejects this stream strategy.
It does not collect a large known-length object as a fallback.

Unknown-length primitive streams are explicitly staged within maxUnknownLengthBytes
(default 8 MiB), additionally bounded by request maxBytes and the single-request
provider cap. Staging/concatenation can retain two payload-sized buffers plus upstream
allocation; configuration is not an RSS ceiling. For larger unknown streams, use
managed multipart with an explicit finite maxBytes and memory policy.

The conservative advertised single/part maximum is 5 GiB minus 5 MiB; overall object
maximum is 5 TiB minus 5 GiB. Keys are capped at 1024 UTF-8 bytes; the adapter's encoded
metadata options are capped at 8192 bytes. A Worker application's ingress, memory,
CPU and lifetime limits apply independently. Listing uses native truncated/cursor
signals and binds its public cursor to storeId and query; it is not a snapshot.

Scopes cancel unread bodies and owned readers. Native get acquisition joins until a
returned response can be safely owned, which can delay interruption. Binding writes
have no general AbortSignal contract: stopping an Effect or closing its source cannot
prove a byte-array mutation stopped remotely. Work can commit after the caller stops
waiting. Request counters bound awaited invocations, not unobservable remote work
continuing after interruption. Retry budgets count binding calls rather than service
internal work. Never turn interruption into a rejected-write assertion.

## Managed and native multipart

```ts
import { Layer } from "effect";

import * as StorageWeb from "@effect-kit/storage/browser";
import { Transfers } from "@effect-kit/storage/transfers";

const ManagedLive = Transfers.layer({
  maxBytes: 1024 * 1024 * 1024,
  memoryBudgetBytes: 64 * 1024 * 1024,
}).pipe(Layer.provideMerge(ProviderLive), Layer.provideMerge(StorageWeb.layer));
```

Basic Storage needs no runtime helper peer. Managed hashing/randomness uses the
web-safe /browser crypto layer and its optional platform-browser peer, independently
executed under workerd in this phase. The provider layer supplies its matching
TransferDriver automatically. It uses one binding, with no hidden S3 fallback.

R2WorkersStorage.multipart exposes initiate, uploadPart, complete and abort. Handles
keep opaque upload IDs Redacted. Repeated stable bytes under one part number are
exercised locally. Completion uses the latest returned part receipt; identical bytes
need not retain the same opaque part ETag. listParts returns typed Unsupported; listUploads is absent.
Native handle reconstruction is not proof an upload exists. Multipart completion
cannot enforce the portable write conditions, so completeConditions is explicitly
empty. The transfer planner rejects conditional multipart before opening the source
or initiating remote work. Small conditional single-request uploads remain supported.

No durable upload handle codec is supplied: UploadSessions.create fails before
fingerprinting/mutation for binding-only multipart uploads. The missing inventory
and conditional-completion guarantees are explicit. Do not attach an S3 codec or
credentials implicitly. Download sessions can use an application-supplied protected
Effect KVS and owned target; the local fixture uses memory and proves no cross-request
persistence, cross-tab ownership or browser UX.

The driver can reconcile an ordinary uncertain commit only from its exact operation
marker and size. Without proof it remains unknown. Native copy and conditional delete
are unavailable; copy can use the existing verified streaming workflow, while moves from R2 fail before copying because safe source deletion authority is
unavailable. They do not silently degrade into unconditional deletion. Signing and administration require
separate authority and are not supplied by this binding.

## Public delivery and verification

`R2WorkersStorage.publicUrls(baseUrl)` is an explicit separate StoragePublicUrls layer
requiring R2WorkersStorage. It validates authority/refs and encodes exact key segments;
dot-segment/versioned refs are unsupported. Resolving a URL changes no visibility.
Bucket domains, CORS, locks, lifecycle and account management retain phase 10/11 scope.

Run `pnpm test:r2:workers` for an owned local workerd bucket. It packs the exact archive,
installs only Effect/platform-browser peers in an isolated app, rejects server imports,
disables outbound network, and executes conformance, streams, multipart and workflows.
No Cloudflare account is used. Reports are in `.artifacts/r2-workers/report.json`.
The acceptance fixture clears its owned local bucket between cases; do not deploy it
or point it at application data. The read-only application example is
`packages/storage/examples/r2-worker.mjs`.

See [phase 6 evidence](evidence/phase-6-r2-workers.md). A real deployed Worker/bucket
lane remains unexecuted: no existing Cloudflare resources or deployment authorization
were supplied. Hosted conditional races/rate limits, Worker production limits and
other compatibility dates require independent qualification.
