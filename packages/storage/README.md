# @effect-kit/storage

Object storage for **Effect v4**. Typed operations, scoped streams, managed transfers
and resumable workflows share one service across cloud providers and local storage.

Provider SDKs, runtime integrations and React bindings have separate optional imports.
Install only the peers your application uses.

## Install

```sh
npm install @effect-kit/storage effect
pnpm add @effect-kit/storage effect
bun add @effect-kit/storage effect
yarn add @effect-kit/storage effect
```

Requires Effect `^4.1.0` and ESM. Node.js integrations require Node **24.22.0+**.
Runtime adapters also target Bun, Deno and browsers with their matching Effect platform
packages. A runtime adapter does not make every provider SDK available on that runtime.

## Quick start

Provide a storage Layer and use the `Storage` service. Memory storage needs no cloud
credentials or optional peers:

```ts
import { Effect } from "effect";

import { Storage } from "@effect-kit/storage";
import { MemoryStorage } from "@effect-kit/storage/testing";

const program = Effect.gen(function* () {
  const storage = yield* Storage;

  const saved = yield* storage.put({
    key: "greetings/hello.txt",
    body: new TextEncoder().encode("Hello from Effect"),
    contentType: "text/plain",
  });

  return yield* Storage.getText({ ref: saved.ref, maxBytes: 1024 });
});

const greeting = await Effect.runPromise(program.pipe(Effect.provide(MemoryStorage.layer())));
console.log(greeting);
```

### AWS S3

```sh
pnpm add @aws-sdk/client-s3
```

Use the official AWS SDK credential chain, or supply its credential provider explicitly.
Keep credentials in your server environment or identity integration:

```ts
import { Config, Effect, Layer } from "effect";

import { S3Storage } from "@effect-kit/storage/s3";

export const StorageLive = Layer.unwrap(
  Effect.gen(function* () {
    return S3Storage.layer({
      bucket: yield* Config.String("STORAGE_BUCKET"),
      region: yield* Config.String("AWS_REGION"),
    });
  }),
);
```

The provider Layer supplies `Storage`, its native provider service and `TransferDriver`.
Reuse that Layer throughout the application. At a non-Effect boundary, build one
`ManagedRuntime` from your application Layer and dispose it at shutdown.

## Operations and streaming

The portable service exposes `put`, `get`, `head`, `delete` and `listPage`.
`Storage.getBytes`, `Storage.getText` and `Storage.exists` provide bounded convenience
operations; `Storage.list` follows pages as an Effect Stream.

`get` acquires a response inside a Scope. Consume its body before the Scope closes:

```ts
import { Effect, Stream } from "effect";

import { Storage } from "@effect-kit/storage";

export const countBytes = Effect.scoped(
  Effect.gen(function* () {
    const storage = yield* Storage;
    const response = yield* storage.get({ key: "assets/archive.bin" });

    if (response._tag === "NotModified") return 0;

    return yield* Stream.runFold(
      response.body,
      () => 0,
      (total, chunk) => total + chunk.length,
    );
  }),
);
```

Upload bodies accept `Uint8Array` or an Effect Stream of byte chunks. Supply
`contentLength` for known-length streamed PUTs. Unknown-length handling is adapter-bound
and limited; managed transfers provide explicit resource and protocol policies.

Object references contain `storeId`, `key` and an optional native version. Preserve the
returned reference when addressing a particular object version. Keys remain opaque;
provider-specific restrictions are validated at that provider boundary.

## Conditions and capabilities

Use `storage.capabilities` to inspect supported ranges, conditions, versioned reads and
listing behavior. Unsupported guarantees fail explicitly:

```ts
import { Effect } from "effect";

import { Storage } from "@effect-kit/storage";

export const createOnly = Effect.gen(function* () {
  const storage = yield* Storage;

  return yield* storage.put({
    key: "locks/claim",
    body: new Uint8Array([1]),
    condition: { _tag: "CreateOnly" },
  });
});
```

`IfMatch` uses an ETag for conditional operations. Range reads accept `Bounded`, `From`
and `Suffix` requests where supported. Listing is generally not a snapshot.

## Managed transfers

Install the matching Effect platform package for your execution runtime:

```sh
pnpm add @effect/platform-node
```

The Node runtime Layer supplies filesystem, path, secure randomness and hashing services.
Compose it with a provider and transfer policy:

```ts
import { Layer } from "effect";

import * as StorageNode from "@effect-kit/storage/node";
import { S3Storage } from "@effect-kit/storage/s3";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";

export function transferLayer(bucket: string, region: string) {
  return Transfers.layer({
    maxBytes: 256 * 1024 * 1024,
    memoryBudgetBytes: 64 * 1024 * 1024,
    partConcurrency: 2,
  }).pipe(Layer.provide(S3Storage.layer({ bucket, region })), Layer.provide(StorageNode.layer));
}

export const upload = Transfers.upload({
  key: "assets/hello.txt",
  source: UploadSource.bytes(new TextEncoder().encode("Hello")),
});
```

`Transfers` provides `upload`, `download`, `copy`, `move`, `uploadMany` and `downloadMany`.
Sources can be bytes, files, one-shot streams or replayable stream factories. Receipts
include strategy, progress and a checksum. Downloads can verify an expected checksum;
`Transfers.downloadFile` stages and verifies bytes before publishing the destination.

A move may return `Copied` when destination verification succeeded but source deletion
failed. Cancellation cannot reverse a committed mutation. Unknown outcomes and incomplete
cleanup remain explicit; a retry never proves the previous attempt did not execute.

## Durable sessions and persistence

Ordinary storage and transfers do not require persistence. Add `UploadSessions` when
uploads or downloads must survive application restarts.

Sessions require protected checkpoints backed by Effect `KeyValueStore`, stable provider
configuration and replayable source bytes. Node, Bun and Deno expose filesystem-backed
`sessionStore` constructors:

```ts
import { Layer, Redacted } from "effect";

import * as StorageNode from "@effect-kit/storage/node";
import { S3Storage } from "@effect-kit/storage/s3";
import { UploadSessions } from "@effect-kit/storage/sessions";
import { Transfers } from "@effect-kit/storage/transfers";

export function sessionLayer(options: {
  bucket: string;
  region: string;
  directory: string;
  key: Redacted.Redacted<Uint8Array>;
}) {
  const transfers = Transfers.layer().pipe(
    Layer.provide(S3Storage.layer({ bucket: options.bucket, region: options.region })),
  );
  const checkpoints = StorageNode.sessionStore({
    directory: options.directory,
    namespace: "uploads",
    key: options.key,
    ownership: "exclusive",
  });

  return UploadSessions.layer({ configurationId: "application-storage-v1" }).pipe(
    Layer.provide(transfers),
    Layer.provide(checkpoints),
    Layer.provide(StorageNode.layer),
  );
}
```

Supply a securely generated **32-byte key**, retain it across restarts, and keep the
checkpoint directory private. `ownership: "exclusive"` means your application guarantees
one process owns the namespace at a time. It is not a distributed lock. Multiple owners
need an appropriate `SessionOwnership` implementation.

Session methods include `create`, `resume`, `complete`, `status`, `reconcile`, `abort`,
`deliverEvents` and `forget`, plus restartable download and batch operations. A session ID
identifies retained progress; it is not an authorization credential. Recovery support
depends on the provider's native protocol.

## Signing and browser uploads

Signing is a separate authority. For S3, install its signing peer and compose the signer
with the same provider Layer:

```sh
pnpm add @aws-sdk/s3-request-presigner
```

```ts
import { Effect, Layer } from "effect";

import { StorageSigning } from "@effect-kit/storage";
import type { ObjectRef } from "@effect-kit/storage";
import { S3Storage } from "@effect-kit/storage/s3";
import { S3Signing } from "@effect-kit/storage/s3/signing";

export function signingLayer(bucket: string, region: string) {
  return S3Signing.layer.pipe(Layer.provide(S3Storage.layer({ bucket, region })));
}

export const signedRead = (ref: ObjectRef) =>
  Effect.flatMap(StorageSigning, (signing) =>
    signing.sign({ ref, method: "GET", expiresInSeconds: 60 }),
  );
```

Signed URLs and headers are redacted bearer credentials. Browser use also requires the
provider's CORS configuration. URL expiry and application intent expiry have different
boundaries; already-issued provider authority may outlive an application session.

For application-authorized uploads, `StorageUploads` handles intents, authorization,
verification and publication. The `/http` entry point integrates those workflows with
Effect HttpApi. `/client` provides the browser client; `/client/atoms` and `/react` provide
Effect Atom state and hooks. The application supplies authentication, authorization,
persistence, private quarantine storage and publication behavior. Provider credentials
stay on the server.

Install the React peers when using the hooks:

```sh
pnpm add react @effect/atom-react scheduler
```

```ts
import { createElement } from "react";

import { RegistryProvider } from "@effect/atom-react";

import { UploadClient } from "@effect-kit/storage/client";
import { UploadAtoms } from "@effect-kit/storage/client/atoms";
import { useUpload } from "@effect-kit/storage/react";

const attachment = UploadAtoms.make({
  layer: UploadClient.layer({ baseUrl: "/storage" }),
  purpose: "attachment",
  context: {},
});

function Attachment() {
  const upload = useUpload(attachment);

  return createElement("input", {
    type: "file",
    disabled: upload.isRunning,
    onChange: (event: React.ChangeEvent<HTMLInputElement>) => {
      const file = event.currentTarget.files?.[0];
      if (file) upload.start(file);
    },
  });
}

export const App = () => createElement(RegistryProvider, {}, createElement(Attachment));
```

This connects to your application-owned storage HTTP API. React requires React 19.
Create upload atoms once per intended upload lifetime and place consumers under the
matching `RegistryProvider`; command results and upload completion state are separate.

## Providers and optional imports

| Import                                                                           | Integration / additional peer                           |
| -------------------------------------------------------------------------------- | ------------------------------------------------------- |
| `/s3`                                                                            | AWS S3; `@aws-sdk/client-s3`                            |
| `/r2`, `/minio`, `/tigris`, `/spaces`, `/b2`, `/wasabi`, `/scaleway`, `/hetzner` | Named S3-compatible profiles; `@aws-sdk/client-s3`      |
| `/r2-workers`                                                                    | Native Cloudflare R2 binding                            |
| `/gcs`                                                                           | Google Cloud Storage; `@google-cloud/storage`           |
| `/azure`                                                                         | Azure Blob Storage; `@azure/storage-blob`               |
| `/azure/identity`                                                                | Azure identity integration; `@azure/identity`           |
| `/supabase`                                                                      | Supabase Storage over Effect HTTP; optional tus support |
| `/supabase/s3`                                                                   | Supabase S3 endpoint; `@aws-sdk/client-s3`              |
| `/vercel`                                                                        | Vercel Blob; `@vercel/blob`                             |
| `/filesystem`                                                                    | Local filesystem; matching runtime Layer                |
| `/testing`                                                                       | Memory storage, fault injection and adapter conformance |
| `/node`, `/bun`, `/deno`, `/browser`                                             | Matching `@effect/platform-*` package                   |

Native Tigris administration uses `@tigrisdata/storage`; Wasabi native administration
uses `fast-xml-parser`. S3 POST policies separately require `@aws-sdk/s3-presigned-post`.
The core import does not load these optional peers.

Additional modules cover lifecycle/version/metadata operations, provider administration,
events, prefix/read-only/routing views, caching with Effect Cache, migrations, mirroring,
content addressing, gzip and pluggable representation encryption. Administration uses
separate authority and does not provision resources during ordinary construction.

## Compatibility and guarantees

Capabilities depend on the service, region, account policy and configured adapter.
Live qualification is scoped to tested configurations and operations. Azure, GCS,
deployed native R2 Workers, Spaces, Wasabi, Scaleway and Hetzner live qualification is
currently deferred. Local emulator success does not certify those hosted services.

Vercel Blob's multipart API lacks public remote abort/inventory and durable handle
support. Supabase resumable uploads require the explicit `resumable` configuration and
appropriate RLS policies. Native versions, retention/governance, event delivery and
browser grants have their own provider-specific boundaries.

The optional age encryption adapter is not independently audited. Deployment key
management, distributed coordination and cross-system exactly-once guarantees remain
application responsibilities. Do not interpret local roundtrip tests as a security audit.

## Errors and observability

Expected failures use `StorageError`, with `reason`, `operation`, `provider` and
`mutationOutcome`. Handle reasons such as `NotFound`, `AccessDenied`,
`PreconditionFailed`, `Unsupported`, `IntegrityMismatch` and `TransportFailure` explicitly.
A timeout after a possible write can have `mutationOutcome: "unknown"`.

Transfers expose bounded progress, diagnostics and resource counters. Effect owns
execution, interruption and tracing; the application configures telemetry exporters.
Avoid logging raw provider credentials, signed URLs or protected checkpoints.

## Contributing and security

See the [repository](https://github.com/effect-kit/storage) for development and testing.
Report bugs through [GitHub issues](https://github.com/effect-kit/storage/issues).
For security-sensitive reports, use GitHub private vulnerability reporting when enabled;
keep credentials and exploit details out of public issues.

Licensed under [Apache-2.0](https://github.com/effect-kit/storage/blob/main/LICENSE).
Copyright 2026 Effect Kit Contributors.
