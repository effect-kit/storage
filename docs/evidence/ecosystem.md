# Ecosystem comparison — 28 October 2026

Reviewed official docs, exact npm package declarations/source and selected upstream
test files. Package archives and selected sources were hashed in ignored research.
Upstream tests were read, not executed. Main-branch test revisions below are
independent evidence from the published package versions, not claims of exact
release-source correspondence.

## Flystorage

`@flystorage/file-storage` 1.2.2, MIT. Its Node-oriented adapter contract accepts
Readable writes and returns stream-based contents, async listings and prepared
upload requests. Configurable path normalization and separate adapters are useful
discoverability patterns. We preserve opaque object keys and explicit native
identity instead of inheriting a file/path normalization model.

Reviewed AWS/Azure test source at `171a7db323e84c2e4e42984770e6dac801bf7daa`,
including owned random prefixes and prepared-request round trips. The registry's
old repository URL redirects to deltic-oss.
[API](https://flystorage.dev/api/),
[source/tests](https://github.com/deltic-oss/flystorage/tree/171a7db323e84c2e4e42984770e6dac801bf7daa/packages).

## Files SDK

`files-sdk` 3.1.2, MIT. Published types already cover Web streams, capabilities,
conditional operations, receipts, retry controls, resume tokens and plugin
capability changes. These are substantive overlapping features; adapter count
or broad API coverage is not a unique Effect Storage claim.

Reviewed capability snapshot and Bun S3 tests at
`3b24e418f514f7c21e0f456fa209eada22cd7959`. Adopt discoverable capability constraints,
composition compatibility and public consumer tests. Effect Storage's proposed
distinction is native Effect services/source errors/resources plus explicitly
qualified durable workflows; those claims require our own implementation/evidence.
[Concepts](https://files-sdk.dev/docs),
[tests](https://github.com/haydenbleasel/files-sdk/tree/3b24e418f514f7c21e0f456fa209eada22cd7959/packages/files-sdk/test).

## unstorage

`unstorage` 2.0.0-beta.0 was the registry's observed latest tag, MIT; a beta tag
is not an adopted dependency. The key/value API normalizes paths and offers
mounts, metadata/raw operations and driver-specific behavior. Raw type/stream
support varies by driver; inspect each rather than infer equivalent object APIs.

Reviewed Azure driver auth tests at
`5781aae96f7f34820a99e447df29ceead97e9210`. Reuse the principle of isolated driver
dependencies and clear disposal. Avoid normalized key identity and transparent
serialization fallbacks for a binary object contract.
[Guide](https://unstorage.unjs.io/guide),
[drivers/tests](https://github.com/unjs/unstorage/tree/5781aae96f7f34820a99e447df29ceead97e9210/test).

## UploadThing

`uploadthing` 7.7.4, MIT. Route builders, middleware, typed upload-completion data
and browser/server integration offer useful workflow DX. Preserve this separation
of server authorization and client upload state while keeping provider ownership
and application publication explicit.

Reviewed adapter middleware and browser smoke source at
`2656a9a5cc69619e57de76d8b5d2aced40c1fa89`; examples include file-size/count constraints.
Do not treat a completion callback alone as authoritative object verification.
[Docs](https://docs.uploadthing.com/),
[tests](https://github.com/pingdotgg/uploadthing/tree/2656a9a5cc69619e57de76d8b5d2aced40c1fa89/packages/uploadthing/test).

## Uppy and tus

`@uppy/tus` 6.0.0 and `tus-js-client` 4.3.1, MIT. Uppy offers browser orchestration;
its tus plugin wraps the upstream protocol client. The request hooks, retry controls
and resume primitives are useful; a UI/plugin runtime is not needed in core Storage.
The local tus probe demonstrated offset resume and controlled retries.

Reviewed Uppy S3 configuration tests at
`fe886ce2e041e64cbfce0b2434d4a894aca7ac48`; they validate signing configuration.
This is not evidence of our browser integration. Keep UI optional and test real
progress/CORS separately.
[Tus integration](https://uppy.io/docs/tus/),
[source/tests](https://github.com/transloadit/uppy/tree/fe886ce2e041e64cbfce0b2434d4a894aca7ac48/packages/@uppy).

## Effect AWS

`@effect-aws/client-s3` 2.0.0, MIT. Observed peer range
`>=4.0.0-beta.66 <5.0.0` includes the selected Effect release. Published source
provides command services/configuration/errors and native client integration.
It is not excluded on an unfounded incompatibility claim.

Reviewed service construction/config/failure tests at
`4b66b753f71f8cb5a573c4a1db4202eb0c7c84de`. Direct AWS adaptation is selected
because Storage owns a smaller portable contract and its retry/stream/resource
policy; no additional wrapper is necessary.
[Source/tests](https://github.com/floydspace/effect-aws/tree/4b66b753f71f8cb5a573c4a1db4202eb0c7c84de/packages/client-s3).

## Decision

Reuse mature wire/auth/signing clients, existing Effect infrastructure and upstream
tus. Deliver simple core DX with explicit capabilities, uncertain outcomes,
bounded resources, durable recovery and provider evidence. Do not claim another
library lacks a feature merely because its quickstart omits it. No competitor
dependency is installed in the production workspace by this comparison.
