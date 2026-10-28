# @effect-kit/storage

Object storage services and workflows for Effect.

Development status: the portable API and memory reference are implemented. Hosted
adapters, managed transfers and durable upload sessions follow in later phases.
Published-package installation instructions will accompany the first release.

## Use storage

Requires the workspace's Node 24.22+ baseline and Effect 4.

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
HTTP helpers and browser clients will use separate entry points alongside their
implementations and tests. Core does not load provider clients.

See the [portable API](../../docs/core.md), [testing tools](../../docs/testing.md),
[architecture decisions](../../architecture/README.md),
[feature catalog](../../docs/features.json) and [phase 2 evidence](../../docs/evidence/phase-2.md).
Memory is process-local, with configurable quotas and current references rather
than retained history. It cannot qualify cloud races, checksums, durability or
native cancellation. Its five primitives have Node 24.22.0 runtime evidence;
no hosted provider support is currently qualified.

## License

Apache-2.0, Effect Kit Contributors.
