# @effect-kit/storage

Object storage services and workflows for Effect.

Development status: foundation only. This package currently exports `ObjectKey` and
`ObjectSize` schemas. The portable Storage service, providers, transfers and upload
sessions are planned and are not available yet.

## Use the foundation

Requires the workspace's Node 24.22+ baseline and Effect 4. Published-package
installation instructions will be finalized with the first release.

```ts
import { Effect, Schema } from "effect";

import { ObjectKey, ObjectSize } from "@effect-kit/storage";

const decodeMetadata = Schema.decodeUnknownEffect(
  Schema.Struct({ key: ObjectKey, size: ObjectSize }),
);

const program = decodeMetadata({
  key: "invoices/example.pdf",
  size: 42,
});

// Run at your application's runtime boundary.
await Effect.runPromise(program);
```

Keys are nonempty opaque strings. The schema preserves slashes, percent sequences
and Unicode without normalization. It does not establish filesystem confinement,
authorization or provider-specific key/header limits.

Sizes are nonnegative safe integer byte counts, including zero. Fractions, infinity,
negative values and unsafe integers fail decoding. Native versions/generations will
remain opaque strings; a byte count is never a native version identifier.

## Package boundaries

Core imports depend only on Effect. Optional provider SDKs, database integrations,
HTTP helpers and browser clients will be added under separate entry points alongside
their implementations and tests. The root export does not load provider clients.

See the repository [architecture decisions](../../architecture/README.md),
[feature catalog](../../docs/features.json) and [support evidence](../../docs/evidence/phase-1.md).
No hosted provider support is currently qualified.

## License

Apache-2.0, Effect Kit Contributors.
