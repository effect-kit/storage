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
