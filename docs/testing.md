# Memory, faults and adapter conformance

Import these tools from `@effect-kit/storage/testing`. They require Effect, without
cloud SDKs, a database driver or Vitest as a runtime dependency.

## Memory storage

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
```

MemoryStorage.make returns a guarded service in Effect. MemoryStorage.layer creates
an independent store per acquisition; intentional Layer reuse within a graph shares
one instance. No module-global object map or revision counter is used.

An automatic store ID is generated through Effect Random. Set storeId to a stable
logical identity when needed, and do not reuse an identity for unrelated namespaces.
Opaque versions and cursors also include per-instance identity. IDs are not secrets
or authorization; test determinism can use Random.withSeed.

Writes consume and validate a source before atomically evaluating conditions and
publishing. Caller buffers (including Node Buffer) and metadata are copied. Failed
or interrupted sources do not publish partial objects. Overwritten objects have
new opaque validators/versions, and downloads retain matching old bytes/metadata
for their acquired response.

Memory supports current pinned references, not historical version storage. A stale
version read is NotFound; deleting a stale version does not delete the replacement.
It computes no content digests and supplies no real signer, retention or durability.

### Limits

| Option           | Default | Meaning                                                |
| ---------------- | ------- | ------------------------------------------------------ |
| maxObjectBytes   | 16 MiB  | One complete object's bytes                            |
| maxTotalBytes    | 64 MiB  | Current stored object bytes                            |
| maxPendingBytes  | 32 MiB  | Concurrent staging and final concatenation reservation |
| maxReadBytes     | 32 MiB  | Bytes pinned by active response scopes                 |
| maxObjects       | 1000    | Current object count, including empty objects          |
| maxKeyBytes      | 4096    | UTF-8 bytes in a stored key                            |
| maxMetadataBytes | 8192    | Encoded content-header/custom-metadata record          |
| chunkSize        | 64 KiB  | Owned upload/read chunk size                           |

Limits are positive safe integers. Staging reserves twice each retained byte before
copying, and releases reservations on every exit. Current-store limits exclude
metadata/map overhead; active reads reserve conservatively even for partial ranges.
Caller allocations/retained results and runtime overhead are not a process RSS
guarantee. Raise read/pending limits deliberately when increasing object limits.

Pages use code-unit lexicographic key ordering, optional common-prefix aggregation,
and at most 1000 entries. Cursors bind instance/store/prefix/delimiter/page limit,
are process-local and are not authenticated grants. Listing is not a snapshot.
Concurrent insertions before a continuation key can be missed; deletions may remove
later entries.

## Scripted faults

```ts
const base = yield * MemoryStorage.make();
const faults =
  yield *
  Faults.make(base, [
    {
      operation: "put",
      afterFailure: { _tag: "TransportFailure", code: "LostResponse" },
    },
  ]);

const result =
  yield *
  Effect.result(
    faults.storage.put({
      key: "example",
      body: new Uint8Array([1, 2, 3]),
    }),
  );
yield * faults.assertDone;
// The call reports unknown; the base store still contains the committed object.
```

Each accepted primitive call consumes exactly one ordered FaultStep. An exhausted,
mismatched or unconsumed script is a test defect. Include `{ operation: "head" }`
for an intended pass-through call. Steps support delayMillis (Effect Clock),
beforeFailure, afterFailure, bodyFailure.afterBytes/reason, and safe cleanup/observer
failure codes. Body faults apply to put/get; cleanup faults apply to scoped get.

Before failures do not call the backend. Post-mutation response failures report
unknown and preserve backend state. Body failures cannot prove a remote rejection;
they model a producer/read failure while preserving the underlying adapter's state.
Observer failures become diagnostics without reversing success. Cleanup diagnostics
do not replace the primary body failure.

Attempts/diagnostics exclude request keys, bodies and metadata. Get attempts become
acquired, then succeed/fail/interrupted when their body runs; unread scoped responses
become closed. Diagnostic codes are caller-supplied safe test values. These tools
simulate faults, not actual cloud cleanup or durable observer delivery.

## Public conformance suite

```ts
const results = yield * runStorageConformance(() => MemoryStorage.make());
```

The framework-independent runner scopes one new fixture per case. A provider factory
can acquire a bucket/prefix/client with Effect.acquireRelease; the caller owns cleanup.
Alternatively use storageConformanceCases in an Effect-aware test runner.

The 13 cases exercise core round trips, streams/overwrite, missing/delete behavior,
atomic create-only and stale writes when advertised, foreign refs, range/NotModified
behavior, scope closure, collection limits, pagination, prefixes and cursor binding.
Missing optional capabilities yield explicit skip results. A failing core case stops
with ConformanceFailure. No skip certifies unsupported-native or real cloud behavior.

Use native provider tests for wire errors, credentials, hosted concurrency, checksums,
remote cleanup and cancellation. The memory suite cannot qualify those semantics.
