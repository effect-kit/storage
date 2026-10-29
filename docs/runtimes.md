# Runtime layers

Choose one canonical runtime module. The workflow engines are shared; these modules
provide platform services and focused integrity/persistence adapters.

| Module     | Platform peer              | Layer services                                             | Additional exports                                                   |
| ---------- | -------------------------- | ---------------------------------------------------------- | -------------------------------------------------------------------- |
| `/node`    | `@effect/platform-node`    | FileSystem, Path, Digest, SecureRandom, TransferHash       | sha256, aes256Gcm, atomicFileStore, fileDownloadTarget, sessionStore |
| `/bun`     | `@effect/platform-bun`     | FileSystem, Path, Digest, SecureRandom, TransferHash       | Same file conveniences as Node                                       |
| `/deno`    | `@effect/platform-deno`    | FileSystem, Path, Digest, SecureRandom, Aead, TransferHash | Same file conveniences as Node                                       |
| `/browser` | `@effect/platform-browser` | Digest, SecureRandom, Aead, TransferHash                   | sha256, aes256Gcm                                                    |

Effect and selected platform development versions are pinned together at 4.1.0.
Install the platform peer for the module you import; the other platform/provider
peers are optional. Core/testing consumers need only Effect. Provider SDKs remain
separate: selecting a runtime helper does not qualify a provider's transport on it.
S3 currently uses Node transport; its runtime evidence is Node-specific.

The workspace and package declare Node 24.22+ for Node execution/tooling. Actual Bun
and Deno helper execution is tested independently of that Node engine declaration;
some package managers may warn about it in alternate runtimes. Effect platform-node
4.1.0 declares a Redis client peer: strict peer installation may require `redis`.
Our Node fixture installs that upstream peer; storage sessions require no Redis server
and do not connect to Redis. This library does not add Redis as a runtime dependency.

## Managed application setup

```ts
import { Effect, Layer } from "effect";

import * as StorageNode from "@effect-kit/storage/node";
import { S3Storage } from "@effect-kit/storage/s3";
import { Transfers, UploadSource } from "@effect-kit/storage/transfers";

const AppLive = Transfers.layer({ maxBytes: 1024 * 1024 * 1024 }).pipe(
  Layer.provideMerge(S3Storage.layer(config.s3)),
  Layer.provideMerge(StorageNode.layer),
);

const upload = Effect.gen(function* () {
  const source = yield* UploadSource.file(config.uploadPath);
  return yield* Transfers.upload({ key: "uploads/video.bin", source });
});

await Effect.runPromise(upload.pipe(Effect.provide(AppLive)));
```

The application supplies configuration, credentials and authorized paths. Basic
Storage programs need only the provider layer. Managed programs add a runtime layer
and policy. Durable programs additionally supply session identity, backing store,
stable protection key and exclusive ownership; see [the full composition](sessions.md).

`Transfers.layer` captures provider/hash/random services once and provides a private
runtime companion for sessions. Reuse that layer value to share admission. Distinct
managed layer instances have separate budgets. Use `Transfers.make` for standalone
managed services; a returned service alone does not install the session companion.

## Hashing and checkpoint protection

`sha256` implements incremental update, finish and non-consuming snapshot. Node/Bun
use native crypto. Deno/browser use `@noble/hashes` 2.4.0: Effect Digest's complete-array
API cannot implement bounded incremental object hashing or cloned prefix snapshots.
This is a maintained, focused runtime dependency, loaded only by the portable helper.
All implementations declare conservative workspace and return full-object hex SHA-256.
Effect Digest remains appropriate for bounded checkpoint/target identity inputs.

Node/Bun `aes256Gcm(secret)` returns CheckpointProtection directly. Deno/browser use
`yield* Runtime.aes256Gcm(secret)` with their layer, capturing Effect Aead and
SecureRandom. Both use the same versioned AES-256-GCM envelope and associated-data
binding. The application supplies a stable `Redacted<Uint8Array>` containing 32 bytes;
the runtime layer does not generate a persistence key. Custom protection remains valid.

Advanced users can supply `TransferHash` with their own TransferDigest implementation.
Its snapshot semantics, algorithm identifiers and declared workspace must be accurate.
No hash state, credentials or cipher key is serialized in a session checkpoint.

## Files and persistence

Node/Bun/Deno share Effect FileSystem/Path algorithms:

- `yield* UploadSource.file(path)` captures FileSystem and TransferHash, fingerprints
  before opening and at EOF, and preserves source errors/environments.
- `yield* Runtime.fileDownloadTarget({ id, path })` captures file/path/hash services,
  binds an owned partial/final path, checks saved prefixes and verifies before rename.
- `yield* Runtime.atomicFileStore(directory)` returns a KeyValueStore using private
  temporary writes, file sync, atomic replacement and directory sync. It requires a
  trusted exclusively owned directory. Hashed filenames cannot recover logical keys:
  enumeration and nonempty prefix deletion fail explicitly.
- `Runtime.sessionStore({ directory, namespace, key, ownership, maximumCheckpointBytes? })`
  returns an UploadSessionStore layer over that backing and runtime protection.

Mutation effects join issued IO before releasing ownership, including on cancellation.
File helpers do not establish filesystem object-store confinement or distributed
leases. Applications retain session IDs, authorize paths, arrange recovery scheduling
and ensure one owner across processes. Browser persistence/targets remain application
contracts; no arbitrary filesystem directory, IndexedDB or cross-tab coordinator is added.

The unpublished paths `/transfers/node`, `/sessions/node` and `/s3/transfers` are
removed, along with endpoint/digest/codec constructor arguments. There are no aliases
or migrations for old development checkpoints.

## Verification and evidence boundaries

```sh
pnpm ci:check
pnpm test:extended
pnpm exec playwright install chromium
pnpm test:runtimes             # requires actual Bun and Deno executables
pnpm test:runtimes browser     # one runtime, if desired
pnpm test:s3:local             # Docker; separate Node/S3 crash and resource lane
```

`STORAGE_DENO_BINARY` can select a Deno executable. The harness uses an isolated local
archive, only the selected platform peer, two declaration-resolution modes without
Node ambient types, and the actual engine. Deno uses explicit read/write/env permissions
and manual node_modules resolution. Browser checks execute in headless Chromium;
the bundled graph rejects Node imports and server SDKs. Reports live under
`.artifacts/runtime/`. A manual GitHub workflow runs the same per-runtime command.

[Current evidence](evidence/pre-phase-6.md) records exact versions and local scope.
Node has fresh-process S3 crash evidence. Bun/Deno smoke reconstructs file backing
in the same process. Browser smoke uses an in-memory protected store and custom
target; reload, IndexedDB, mobile browsers and direct-upload UX remain separate work.
No helper result implies AWS, R2, other provider/runtime combinations, all OSes,
power-cut safety or release readiness.
