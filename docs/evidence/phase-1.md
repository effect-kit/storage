# Phase 1 evidence — 28 October 2026

Scope: foundation and isolated transport probes. No production provider adapter,
durable session store or hosted provider is qualified by this work.

## Environment and dependency pins

Node 24.22.0, pnpm 12.14.0, TypeScript 7.0.2 and Effect 4.0.6.
Probes ran on the local macOS host; Docker Engine reported 29.8.2.
Provider SDKs were installed only in an ignored standalone research workspace.
The production workspace added no dependencies.

| Probe dependency                                 | Version      |
| ------------------------------------------------ | ------------ |
| AWS client, request presigner and presigned POST | 3.1158.0     |
| @google-cloud/storage                            | 8.4.0        |
| @azure/storage-blob                              | 12.34.0      |
| @supabase/storage-js                             | 2.118.0      |
| @vercel/blob                                     | 2.8.2        |
| tus-js-client                                    | 4.3.1        |
| @effect/sql-pg                                   | 4.0.6        |
| @cloudflare/workers-types                        | 5.20261027.1 |

## Compiled contracts

Strict NodeNext compilation with the installed release verified public APIs for
S3 request conditions/signing/scoped body acquisition, GCS generation/offset
options, Azure ranges/leases/blocks, Supabase streaming, Vercel's read union,
tus request hooks and the PostgreSQL Layer.

A source type fixture preserves Stream error/environment parameters through
consumption and rejects erasing an Effect service requirement. A separate Workers
types compilation narrows missing/body/conditional responses and confirms that
the binding does not expose a signer or remote multipart part listing.

Both compiler invocations passed, including expected-error assertions.
This does not execute a Worker or establish a complete public Storage interface.

## Local runtime probes

All four probe groups passed after resolving probe setup issues; 15 named
observations were retained with exact dependency pins.

- **S3 protocol / RustFS:** streamed upload and same-response metadata/body;
  concurrent create-only requests with one winner; multipart create, part upload,
  list, conditional completion and abort; accepted presigned GET; scoped partial
  body disposal; Effect interruption of an outstanding SDK request; reevaluation
  of expiring credentials. This is RustFS behavior, not hosted AWS/R2/MinIO evidence.
- **Native controlled HTTP:** GCS native response headers and resumable creation
  preconditions; Azure response body/ETag/version plus stage/conditioned commit;
  Supabase SDK and Effect HTTP response shape; Vercel normal/304/ranged response.
  Anonymous/custom endpoints were used only on loopback test servers.
- **PostgreSQL:** compatible Effect driver connects to PostgreSQL 17.7, one
  revision update wins against a stale revision, and a failed transaction rolls
  back. The probe deliberately uses one connection; distributed leases/fencing
  and crash-safe workflows remain phase 5 work.
- **tus:** resumes a Buffer source at a HEAD-reported offset, uploads bounded
  PATCH chunks and invokes per-request hooks with internal retries disabled.
  This is a controlled protocol server, not hosted Supabase acceptance.

## Findings that affect implementation

1. GCS 8.4.0 coerces File generation options through Number: passing the string
   `9007199254740993` produced query value `9007199254740992`. Native response
   headers retained the original string. Preserve opaque identities and reject
   an unsafe SDK path or use lossless authenticated HTTP.
2. A GCS File already bound to a generation affects resumable creation conditions.
   Create-only creation must use the intended unbound destination and native
   generation-match condition; test version/condition combinations explicitly.
3. Supabase 2.118.0 `download().asStream()` returns data/error but no same-response
   headers. Effect HttpClient supplies those headers and streamed bytes together,
   supporting the selected native REST integration.
4. Vercel 2.8.2 returns `statusCode: 200` for a real HTTP 206 response while retaining
   Content-Range. Its adapter must validate actual returned range metadata;
   the SDK result alone cannot certify a complete-body response.
5. tus 4.3.1 Node types accept Buffer/Blob/File/reader inputs, not bare Uint8Array.
   Adapt byte inputs at the protocol boundary. Keep its retries under the logical
   transfer budget rather than nesting uncontrolled retry loops.

These observations come from the pinned published SDKs and controlled responses.
Recheck them on upgrades. Public-source references:
[GCS File](https://github.com/googleapis/google-cloud-node/tree/main/handwritten/storage),
[Supabase Storage](https://github.com/supabase/storage-js),
[Vercel Blob](https://github.com/vercel/storage),
[tus client](https://github.com/tus/tus-js-client).

## Fixtures and reproducibility

The fixture choices/digests and trust boundary are in
[ADR 005](../../architecture/decisions/005-persistence-and-fixtures.md).
Disposable credentials were generated per run, ports bound to loopback, and
objects/containers limited to phase-owned resources. Raw scripts, SDK/source
snapshots, isolated lockfile and JSON results remain ignored local research.
They are not required by CI or installed consumers.

Local commands executed:

```sh
# Local investigation workspace; intentionally not part of a fresh clone.
research/phase-1/node_modules/.bin/tsc --noEmit -p research/phase-1/tsconfig.json
research/phase-1/node_modules/.bin/tsc --ignoreConfig --noEmit --target ES2024 \
  --module NodeNext --types @cloudflare/workers-types \
  --typeRoots research/phase-1/node_modules --skipLibCheck --strict \
  research/phase-1/src/workers.ts
node research/phase-1/run-probes.mjs
```

## Maintained verification and open gates

The renamed package's three object-schema tests and ten tooling tests pass.
Catalog checks cover all 47 features and 16 targets. Packed consumers execute
outside the workspace without cloud/SQL peers and typecheck production
declarations with NodeNext and Bundler resolution; Publint and ATTW pass.

Repository commands: `pnpm format`, `pnpm ci:check` and `pnpm test:extended`.
The latter is now real artifact verification, not an echo placeholder.

Not run: hosted provider/RLS/signing-identity/retention acceptance, real browser
CORS/progress, Worker deployment, Bun provider behavior, OS filesystem matrix,
large-transfer budgets/soak, durable session/lease recovery, release provenance
and registry smoke. These are scheduled implementation/qualification gates,
not phase 1 support claims. The template release check/smoke commands remain pending.
