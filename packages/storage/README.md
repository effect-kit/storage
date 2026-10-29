# @effect-kit/storage

Object storage services and workflows for Effect.

Development status: the portable API, memory reference and official-SDK S3 adapter
are implemented. S3 has local protocol evidence; hosted AWS acceptance is pending.
Managed transfers and durable sessions are implemented for the documented ownership profiles.
Published-package installation instructions will accompany the first release.

## Use storage

Requires Effect 4.1 within the declared peer range. Workspace tooling uses Node
24.22+. Runtime helpers have separately measured [support boundaries](../../docs/runtimes.md).

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

See the [portable API](../../docs/core.md), [testing tools](../../docs/testing.md),
[architecture decisions](../../architecture/README.md),
[feature catalog](../../docs/features.json) and [phase 2 evidence](../../docs/evidence/phase-2.md).
Memory is process-local, with configurable quotas and current references rather
than retained history. It cannot qualify cloud races, checksums, durability or
native cancellation. Its five primitives have Node 24.22.0 runtime evidence;
no hosted provider support is currently qualified.

## AWS S3

Import S3Storage from `@effect-kit/storage/s3` and provide
`S3Storage.layer({ bucket, region })` to the same portable program. Install the
optional official `@aws-sdk/client-s3` peer. Signing and POST policies use
`/s3/signing` and `/s3/post`, with their respective official optional peers.

See [S3 setup, ownership and limits](../../docs/s3.md). Known-length streams use
backpressure; unknown-length streams stage within an explicit per-operation limit.
`pnpm test:s3:local` runs owned RustFS protocol acceptance without an AWS account.

## Managed transfers

Use `Transfers.layer(policy?)` from `/transfers`, provide your provider layer and
the selected `/node`, `/bun`, `/deno` or `/browser` runtime layer. S3 supplies its
transfer driver automatically; policy contains execution choices.
`yield* UploadSource.file(path)` captures filesystem/hash services.
The engine owns shared memory/request budgets, retained-part retries, pinned download
continuation, verified native/streamed copy, explicit move outcomes and lazy batches.
See [the transfer API and resource policy](../../docs/transfers.md) and
[measured local evidence](../../docs/evidence/phase-4.md). The engine is in-process;
persistent restart and required events use the optional sessions entry point.

## Durable sessions

`/sessions` adds protected checkpoints over an application-provided Effect KeyValueStore.
Runtime `/node`, `/bun` and `/deno` add AES-GCM, an atomic filesystem backing and
restartable file targets. Compose `UploadSessions.layer(options)` with the managed
layer and `StorageNode.sessionStore(options)` (or your Effect KVS/protection).
PostgreSQL is not required. Base sessions require exclusive application ownership across
processes; no generic distributed CAS/lease guarantee is claimed. See
[session APIs and limits](../../docs/sessions.md), [restart evidence](../../docs/evidence/phase-5.md),
[current runtime evidence](../../docs/evidence/pre-phase-6.md),
and the repository example in `examples/durable-upload.mjs`.

## Filesystem storage

`/filesystem` exports FilesystemStorage.make/layer({ root, storeId }). Supply a
Node/Bun runtime layer; the provider supplies Storage and a primitive transfer driver.
Streamed envelopes publish bytes/metadata together, with atomic create-only and
scoped reads. See [layout, trust and limits](../../docs/filesystem.md) and
[local Node/Bun crash evidence](../../docs/evidence/phase-6-filesystem.md).

## License

Apache-2.0, Effect Kit Contributors.
