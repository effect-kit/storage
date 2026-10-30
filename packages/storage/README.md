# @effect-kit/storage

Object storage services and workflows for Effect.

Development status: the portable API, memory reference and official-SDK S3 adapter
are implemented alongside native GCS/Azure, filesystem and R2 Workers providers.
Cloud support has local protocol evidence; hosted qualification is pending.
Managed transfers and durable sessions are implemented for the documented ownership profiles.
Published-package installation instructions will accompany the first release.

## Use storage

Requires Effect 4.1 within the declared peer range. Workspace tooling uses Node
24.22+. Runtime helpers have separate qualification checks.

```ts
import { Effect } from "effect";

import { Storage } from "@effect-kit/storage";
import { MemoryStorage } from "@effect-kit/storage/testing";

const program = Effect.gen(function* () {
  const storage = yield* Storage;
  yield* storage.put({
    key: "notes/example.txt",
    body: new TextEncoder().encode("hello"),
    contentType: "text/plain",
    condition: { _tag: "CreateOnly" },
  });

  return yield* Storage.getText({ key: "notes/example.txt", maxBytes: 1024 });
}).pipe(Effect.provide(MemoryStorage.layer()));

// Run at your application's runtime boundary.
await Effect.runPromise(program);
```

Storage supplies put, scoped get, head, delete and listPage. Helpers add exists,
explicitly bounded byte/text collection, lazy listing and ordered per-key delete
results. Upload streams preserve caller error/environment types. Read bodies are
single-use and tied to their acquisition scope. Signing, public URLs and native
copy use separate capability services.

Keys preserve slashes, percent sequences and Unicode normalization; empty keys,
NUL and malformed Unicode fail decoding. Sizes are nonnegative safe integers;
native versions/generations remain opaque strings. Keys establish neither
filesystem confinement nor authorization.

## Package boundaries and evidence

Core and `/testing` depend only on Effect. Provider SDKs, database integrations,
HTTP helpers and browser clients use separate entry points alongside their
implementations and tests. Core does not load provider clients.

Memory is process-local, with configurable quotas and current references rather
than retained history. It cannot qualify cloud races, checksums, durability or
native cancellation. Its five primitives have Node 24.22.0 runtime evidence;
no hosted provider support is currently qualified.

## AWS S3

Import S3Storage from `@effect-kit/storage/s3` and provide
`S3Storage.layer({ bucket, region })` to the same portable program. Install the
optional official `@aws-sdk/client-s3` peer. Signing and POST policies use
`/s3/signing` and `/s3/post`, with their respective official optional peers.

Known-length streams use backpressure; unknown-length streams stage within an
explicit per-operation limit.
`pnpm test:s3:local` runs owned RustFS protocol acceptance without an AWS account.

## Managed transfers

Use `Transfers.layer(policy?)` from `/transfers`, provide your provider layer and
the selected `/node`, `/bun`, `/deno` or `/browser` runtime layer. S3 supplies its
transfer driver automatically; policy contains execution choices.
`yield* UploadSource.file(path)` captures filesystem/hash services.
The engine owns shared memory/request budgets, retained-part retries, pinned download
continuation, verified native/streamed copy, explicit move outcomes and lazy batches.
The engine is in-process; persistent restart and required events use the optional
sessions entry point.

## Durable sessions

`/sessions` adds protected checkpoints over an application-provided Effect KeyValueStore.
Runtime `/node`, `/bun` and `/deno` add AES-GCM, an atomic filesystem backing and
restartable file targets. Compose `UploadSessions.layer(options)` with the managed
layer and `StorageNode.sessionStore(options)` (or your Effect KVS/protection).
PostgreSQL is not required. Base sessions require exclusive application ownership across
processes; no generic distributed CAS/lease guarantee is claimed. See the repository
example in `examples/durable-upload.mjs`.

## Filesystem storage

`/filesystem` exports FilesystemStorage.make/layer({ root, storeId }). Supply a
Node/Bun runtime layer; the provider supplies Storage and a primitive transfer driver.
Streamed envelopes publish bytes/metadata together, with atomic create-only and
scoped reads.

## Native R2 Workers

`/r2-workers` exports R2WorkersStorage.make/layer({ bucket: env.BUCKET, storeId }).
It uses the binding directly, supplies its transfer driver and imports no cloud SDK.
Native multipart lacks inventory and conditional completion; durable multipart upload
sessions fail closed.
Hosted R2 acceptance is pending; local tests need no Cloudflare account.

## License

Apache-2.0, Effect Kit Contributors.

## Native GCS and Azure

`GcsStorage.layer({ bucket })` from `/gcs` captures the official Google SDK and
supplies native offset uploads. `AzureStorage.layer({ containerUrl, credential })`
from `/azure` supplies native named blocks. Both provide the same portable Storage,
managed Transfers and protected UploadSessions composition. Native strategy receipts
require pinned SHA-256 readback; durable restart never adopts remote state by key alone.

Signing is explicit through `/gcs/signing`, `/gcs/post`, or `/azure/signing`.
`/azure/identity` alone requires the optional identity SDK.
`pnpm test:native:local` needs Docker and no cloud account.

## Supabase and Vercel

`/supabase` supplies native REST Storage without SDK peers. Explicit user/service
credentials preserve authority; opt-in tus adds fixed-chunk transfers and protected
restartable sessions. `/supabase/signing` supplies native download/upload grants;
`/supabase/s3` is a separate AWS-client profile with explicit S3 credential modes.

`/vercel` uses the optional official `@vercel/blob` peer. Configure public/private
store identity and disable SDK retries with `VERCEL_BLOB_RETRIES=0`. Native signing,
constrained upload grants, browser SDK uploads and authenticated server callbacks
have focused `/vercel/signing`, `/vercel/client` and `/vercel/uploads` entry points.
Multipart inventory/abort is unavailable; durable Vercel uploads fail before initiation.

These adapters have local protocol evidence; hosted RLS/CDN/OIDC qualification remains pending.

Product documentation will be published on the separate landing site after implementation.

## Named S3-compatible providers

`/r2`, `/minio`, `/tigris`, `/spaces`, `/b2`, `/wasabi`, `/scaleway` and `/hetzner`
export `R2Storage`, `MinioStorage`, `TigrisStorage`, `SpacesStorage`, `B2Storage`,
`WasabiStorage`, `ScalewayStorage` and `HetznerStorage`. Install the optional official
`@aws-sdk/client-s3` peer. Each provides Storage, its named service, S3Storage and
TransferDriver over one owned client. Use the existing `/s3/signing` and `/s3/post`
services with their optional signing peers; profile restrictions also apply there.

All constructors require explicit `bucket` and `credentials` (an AWS credential
identity or refresh provider). R2 adds `accountId` and optional `jurisdiction`;
Tigris requires `placement`; Spaces requires `region` and `tier`. B2, Wasabi and
Scaleway require `region`. Hetzner also requires stable `versioning`; MinIO requires
`endpoint`, `region`, `edition`, `serverVersion` and `versioning`. These describe an
existing deployment; constructors do not provision or inspect account configuration.
Trusted endpoint overrides support gateways. HTTPS is required except loopback fixtures.

See `examples/compatible.mjs`: set `STORAGE_COMPATIBLE_PROVIDER`, nonsecret provider
settings as `STORAGE_COMPATIBLE_CONFIG` JSON, explicit access/secret key environment
variables and `STORAGE_COMPATIBLE_KEY`. It writes that key and reads it back.

Conditions/checksums with unresolved semantics fail before source reads. Scaleway
uses 1,000 parts; other profiles currently use a conservative 10,000-part adapter
bound. All profiles cap objects at 5 decimal TB and parts/single puts at 5 decimal GB;
these are adapter ceilings, not claims about every native service maximum. SDK checksum
protection remains enabled for Scaleway/Wasabi; other profiles use required-only SDK
checksums and currently reject explicit unqualified combinations. Managed verification
and optional protected sessions remain available. Session handles bind configuration.
Local RustFS results do not qualify any hosted provider or an actual MinIO edition.

## Lifecycle and administration

`StorageVersions`, `StorageMetadata` and `StorageTags` are optional services from the
root or `/lifecycle`. Supply only the capabilities the application uses:

```ts
import { Effect, Layer } from "effect";

import { StorageVersions } from "@effect-kit/storage";
import { S3Storage } from "@effect-kit/storage/s3";
import { S3Versions, S3Tags } from "@effect-kit/storage/s3/lifecycle";

const storage = S3Storage.layer({ bucket: "application-uploads", region: "us-east-1" });
const lifecycle = Layer.merge(S3Versions.layer, S3Tags.layer).pipe(Layer.provide(storage));
const history = Effect.flatMap(StorageVersions, (versions) =>
  versions.listPage({ prefix: "uploads/", limit: 100 }),
).pipe(Effect.provide(lifecycle));
```

Versions expose lazy `list`, `listPage`, `deleteVersion`, `removeDeleteMarker` and
`restore`. Deletion requires an exact immutable version; S3 marker removal is a
separate operation. Restore copies an exact source and requires a destination
condition. GCS uses generation conditions; its portable restore supports create-only.
S3 metadata updates copy at most 5 GB and require current destination `IfMatch`.
They retain reported encryption, retention, tags and storage class. Source and
destination copy conditions are sent together; compatibility profiles without
qualified conditional copy reject metadata updates. GCS metadata requires both
generation and metageneration. Azure custom metadata requires current `IfMatch`;
`AzureProperties.setHeaders` updates system headers separately with its own CAS.
`replace` replaces custom metadata while retaining omitted system headers; `merge`
patches custom metadata, with `null` removing a field.

| Entry point                           | Services and operations                                                                                       |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `/s3/lifecycle`                       | S3Versions, S3Metadata, S3Tags                                                                                |
| `/s3/governance`                      | S3ObjectLock: retention, legal holds, explicit governance bypass                                              |
| `/s3/archive`                         | S3Archive: request/status/await readiness; S3StorageTier: encryption observations and conditional tier copies |
| `/gcs/lifecycle`                      | GcsVersions, GcsMetadata, GcsRecovery: soft-deleted generation restore                                        |
| `/gcs/governance`                     | GcsProtection: holds/retention/encryption observations; GcsStorageTier: conditional rewrites                  |
| `/azure/lifecycle`                    | AzureVersions, AzureMetadata, AzureProperties, AzureTags                                                      |
| `/azure/governance`, `/azure/archive` | AzureProtection; AzureArchive: tier/rehydration status and native undelete                                    |
| `/tigris/native`                      | TigrisAdministration: configuration/snapshots/forks; TigrisRecovery: soft-delete and archive                  |
| `/wasabi/native`                      | WasabiNative: exact-key MOVE/compose/append; WasabiCompliance: legacy bucket policy                           |

Every `/provider/admin` module captures separate, explicitly supplied management
authority. S3/named S3 admin layers own a separate SDK client. GCS admin accepts an
explicit SDK authentication configuration; Azure container administration and Azure
ARM `/azure/account-admin` are separate. R2 uses a Cloudflare account token; Supabase
requires service-role authority; `/b2/admin` uses a native B2 application key.
Construction does not provision resources or change configuration.

S3 and GCS configuration expose `get`, `plan` and `apply`. Plans are bound to their
issuing service, checked for mutation, consumed once and checked for stale state.
S3 requires explicit `allowUnconditional: true`: its preflight is best effort,
because bucket configuration has no general CAS. It returns per-step outcomes
after partial changes. GCS applies native metageneration CAS. Other native control
planes expose explicit replacements and accepted/observed results. No multi-service
configuration transaction or automatic rollback is implied.

Releasing holds, bypassing governance and changing protection require explicit
typed intent. Native retention may be irreversible. Archive requests, rehydration,
rewrites and tier copies can incur storage/retrieval charges; an accepted request
does not establish readiness. GCS Archive remains online. Azure `deleteEmpty`
rejects because its container deletion API lacks an atomic nonempty guard.

Install `@tigrisdata/storage` only for `/tigris/native`: the official SDK provides
native bucket configuration and fork controls; our scoped AWS client handles
snapshot/bucket mutations without hidden retries. Tigris restore uses Days; AWS-only
restore tiers are rejected. `TigrisAdministration` takes an
explicit HTTPS endpoint and refreshable credential function; organization session
credentials also require `organizationId`. Tigris bucket settings readback is
Redacted because native migration/notification settings can contain credentials. Install `fast-xml-parser` only for
`/wasabi/native`: it validates bounded XML receipts and encodes native compose and
legacy Compliance requests. Core and ordinary S3 imports require neither peer.

## Provider events

`/s3/events`, `/gcs/events`, `/azure/events`, `/r2/events` and `/tigris/events` decode
native payloads without provider SDKs. They check the selected namespace, preserve
opaque versions, decode keys according to the native format and retain raw input
inside `Redacted`. Parsing does not authenticate a webhook or queue message.

`/events` supplies `StorageEventInbox.layer({ namespace, ownership: "exclusive" })`
over an application-provided Effect KeyValueStore. `makeEventProcessor` requires an
authentication callback before delivery. Authenticate the transport, bound input
size and validate its delivery identity in that callback. The inbox serializes
per-store delivery, rejects duplicates and orders native per-object sequencers.
Unsequenced event IDs are retained in the application's KVS; choose a namespace and
backing-store retention horizon appropriate to the transport's replay window.
Applications must give one process exclusive ownership; independent KVS instances
or processes do not gain a distributed lock.

Delivery is at least once. Use the supplied idempotency key in an application
outbox/transaction: a crash between callback completion and inbox acknowledgement
can redeliver. `/session-events` supplies `sessionEventDelivery({ resolve, outbox })`:
resolve an event through an application-owned index, bind its object identity, then
reconcile the existing protected session and deliver its durable events. Provider
events never directly mark sessions complete or replace a committed object outcome.
