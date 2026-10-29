# Phase 3 evidence

Date: 2026-10-29. Node 24.22.0; Effect/@effect/vitest 4.0.6; TypeScript 7.0.2.
Official client-s3, s3-request-presigner and s3-presigned-post: 3.1159.0.
S3 service API: 2006-03-01, general-purpose bucket profile.

## Implementation and checks

The S3 entry points implement five portable operations, native copy/part copy,
ordered per-key batch deletion, multipart lifecycle/inventory, query signing,
POST policies and explicit public URL resolution. Public constructors retain
owned/borrowed client lifetime, caller source E/R and scoped single-use downloads.
Native options preserve identity, conditions, checksum and configured encryption
semantics. See [the S3 contract and limits](../s3.md).

The offline package suite passes 75 tests; the catalog/archive suite passes 10 tests.
Controlled HTTP tests run against the official SDK serializer/deserializer,
rather than replacing client.send. They exercise
wire conditions/ranges/headers/versions, ambiguous missing/denied mapping, checksum
corruption, source failures/defects, interruption, owned/borrowed clients, credential
refresh, one-attempt response loss, embedded HTTP 200 failures, mixed delete results,
multipart ordering/cursors and signature/header/expiry/redaction/POST policy behavior.

Packed consumers first run core/testing without AWS peers, then S3 with only its
client peer, then S3/signing/POST with explicit peers. Runtime requests, public E/R
fixtures and actual packed NodeNext declarations pass. The browser/Bundler core
consumer remains SDK-free. Publint and ATTW pass the supported ESM profile; legacy
CommonJS/node10 resolution is outside that profile.

Required checks: `pnpm format`, `pnpm ci:check`, `pnpm test:extended` and
`pnpm test:s3:local`. Evidence artifacts are generated under ignored `.artifacts/`;
no runtime or CI input depends on ignored research.

## Executed local provider acceptance

Fixture: rustfs/rustfs@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2.
Configuration: isolated local HTTP endpoint, disposable generated credentials,
versioned bucket, default adapter checksums, one transport attempt.

Six acceptance tests pass, including all 13 public conformance cases within the
conformance test. The remaining tests exercise real protocol streaming/version
reads/deletes, native copy, multipart copy/parts/inventory/conditional completion,
signed GET/HEAD/PUT/DELETE/parts, rejected changed headers/expired grants/checksums,
and POST form acceptance. Per-case cleanup removes only unique owned prefixes'
uploads, versions and markers. The runner removes its owned container and credentials.

One AWS-only authority/region test is explicitly skipped in local mode. The fixture
does not establish actual AWS IAM, region/account routing, KMS/SSE-C enforcement,
credential-chain deployment, hosted concurrency/consistency, CORS or billing behavior.
RustFS is a protocol fixture, not certification of AWS or of a named MinIO target.

## Open gate and handoffs

The owner requested leaving real AWS acceptance pending on 2026-10-29. No AWS
credentials/profile was available, and no AWS account resource was accessed.
AWS catalog rows remain implemented-not-live-verified. Phase 3 implementation and
local validation are delivered; its original hosted acceptance gate remains open.

The maintained `test:s3:aws` suite can exercise an authorized existing versioned
bucket/prefix later. It must pass before hosted AWS support is marked verified.
No post-release scope reduction is implied by postponing this run.

Managed strategies, aggregate budgets, replay/retry and cleanup coordination remain
phase 4; durable sessions phase 5; named S3 profiles phase 9; governance/admin phase 10;
browser/direct-upload verification phase 11; additional runtime/provider hardening
and exact release tooling phase 14. Existing release placeholders remain unimplemented.

## Primary references checked during implementation

- [AWS SDK source](https://github.com/aws/aws-sdk-js-v3), installed 3.1159.0 declarations/source.
- [Multipart limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html).
- [Copy request and embedded response errors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html).
- [Presigned URL lifetime and credential limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).
- [Official SDK checksum behavior](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/s3-checksums.html).
