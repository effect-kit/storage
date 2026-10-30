# Phase 8 evidence

Date: 2026-10-30. Baseline: Node 24.22.0, Effect 4.1.0, TypeScript 7.0.2,
@vercel/blob 2.8.2, AWS SDK 3.1159.0. No hosted Supabase project or Vercel store
was accessed. Tests use owned loopback protocol fixtures and synthetic credentials.

Supabase storage-js 2.118.0 and tus-js-client 4.3.1 were inspected/probed during
transport research. The SDK stream result lacks response metadata; public tus Upload
owns the loop rather than independent checkpointed requests. The adapter uses Effect
HttpClient REST and focused tus 1.0 operations, retaining the established transfer
engine as the sole retry/journal/resource owner. Neither SDK is a runtime dependency.

Local suites: `packages/storage/test/supabase/*.test.ts` and `test/vercel/*.test.ts`.
They execute actual Effect HTTP and Vercel/AWS SDK traffic, not mocked SDK methods.
Supabase covers core conformance, scoped metadata/ranges, error classification,
credential refresh, grants, fixed tus chunks, empty/partial uploads, response loss,
changed-source rejection, protected checkpoint reconstruction and S3 JWT/checksums.
Vercel covers exact encoded paths, private cache bypass, conditions, SDK 206/304
mapping, single attempts, store-bound token refresh, signing, ordered multipart,
missing inventory/abort, unknown completion and signed/idempotent callback handling.
The Web upload bridge separately checks late source errors and bounded staging.

These fixtures do not qualify native RLS, cloud limits, CDN convergence, real signing
authority, OIDC refresh, provider tus expiry/termination or hosted callback delivery.
SDK declarations, compilable consumers, protocol behavior and hosted semantics remain
separate evidence. Native versioning and advanced administration stay outside this
phase. Full browser/CORS/upload acceptance belongs to phase 11.

Supabase storage/signer imports require no SDK peers. Vercel imports require only its
SDK, while Supabase S3 requires only the AWS client. Extended artifact checks compile
isolated NodeNext/Bundler consumers and execute the production tarball entry points.
Browser bundle checks and hosted command details are recorded with their tooling.

Hosted gates: P8-SUPABASE-01 (two-user RLS/expired JWT/privileged paths and tus),
P8-VERCEL-01 (actual public/private/cache/conditions/grants and callback authority).
Until those run, catalog cells remain implemented-not-live-verified.

Final local verification:

- `pnpm format` and `pnpm ci:check`: 183 package tests, 10 tooling tests, catalogs,
  lint, source/test/tooling types and tsdown build.
- `pnpm test:app:local`: exact production archives, publint/ATTW, isolated chosen-peer
  NodeNext/Bundler consumers, browser bundle construction and Chromium initialization,
  five actual SIGKILL/restarts over protected filesystem checkpoints, and executed
  Supabase put/create/resume/status plus Vercel put examples.
- The native SDK client-to-Effect-handler HTTP test proves that data-plane requests
  carry a constrained client token and that a client acknowledgement does not invoke
  application publication. It uses Node transport; browser upload/CORS remains open.
- `pnpm test:s3:local`: 10 passes/one AWS-only skip, including 320 MiB/32 MiB resource
  and separate-process recovery regressions after checksum-profile changes.
- `pnpm test:native:local`: 15 Azurite cases and nine GCS/Azure SIGKILL/restarts after
  shared completion/validator changes. Azurite's previously documented API-version
  bypass remains explicit; it is not Azure cloud qualification.
- `actionlint .github/workflows/app-storage-local.yml` and `git diff --check`.

The local summary is `.artifacts/app-storage-local.json`. The manual workflow uploads
only that whitelisted JSON, with no credentials, checkpoints or browser bundles.
Hosted test setup is in [the example guide](../app-storage-examples.md); no hosted
lane ran because resources, credentials and an authorized prefix were not configured.
