# Effect Kit Storage

Effect-native object storage for applications, workers and upload backends.

The package is under development. The portable Storage service provides uploads,
scoped downloads, metadata, deletion and pagination. The `/testing` entry point
provides memory storage, fault scripts and a public adapter conformance suite.
The official-SDK S3 adapter adds native copy, multipart and signing. It has local
protocol evidence; hosted AWS acceptance is pending. Managed transfers add bounded
uploads, pinned downloads, verified copy/move and lazy batches. Durable sessions reuse Effect persistence with explicit exclusive ownership.
Runtime layers for Node, Bun, Deno and browsers supply hashing and platform services.
The filesystem provider adds atomic object envelopes on an application-owned directory.
Native R2 Workers uses caller-owned bindings and has local workerd acceptance.
Native GCS and Azure adapters add offset/block uploads, signing and protected restartable sessions, with local SDK/protocol evidence.

## Package

The library lives in [packages/storage](packages/storage/README.md) and will publish as
`@effect-kit/storage`. Effect is a peer dependency. Provider SDKs, persistence drivers
and server/browser integrations stay behind focused optional entry points as those
features are implemented.

## Development

Use Node from `.node-version` and pnpm from `packageManager`:

```sh
pnpm install --frozen-lockfile
pnpm format
pnpm ci:check
pnpm test:extended
```

| Command                          | Purpose                                                        |
| -------------------------------- | -------------------------------------------------------------- |
| `pnpm dev`                       | Watch package builds                                           |
| `pnpm build`                     | Build ESM and declarations with tsdown                         |
| `pnpm format` / `format:check`   | Oxfmt formatting                                               |
| `pnpm lint`                      | Oxlint, including tooling                                      |
| `pnpm typecheck`                 | Source, tests, tooling and configurations                      |
| `pnpm test`                      | Offline catalog/archive-policy and package tests               |
| `pnpm catalog:check`             | Validate complete feature/provider inventories and evidence    |
| `pnpm ci:check`                  | Formatting, catalogs, lint, types, tests and build             |
| `pnpm check:package`             | Publint and Are the Types Wrong                                |
| `pnpm test:artifacts`            | Build, pack and test isolated production consumers             |
| `pnpm test:runtimes [runtime]`   | Exact packed runtime helpers and isolated declarations         |
| `pnpm test:filesystem [runtime]` | Real Node/Bun file, process race and crash acceptance          |
| `pnpm test:r2:workers`           | Isolated packed Worker and owned local R2 binding acceptance   |
| `pnpm test:native:local`         | Owned Azurite, SDK protocol faults and native process restarts |
| `pnpm test:native:hosted`        | Explicit existing GCS/Azure test namespace qualification       |
| `pnpm test:extended`             | Artifact checks; add later manual suites as they exist         |

The artifact lane installs exact local tarballs with Effect and TypeScript from the
local pnpm store in offline mode. A frozen workspace install seeds the required store;
core/testing consumers install no provider peers. A separate S3 consumer installs
its explicit client/signing peers. GCS and Azure consumers each install only their
chosen native SDK, with identity isolated from ordinary Azure imports. Consumers run outside the workspace with NodeNext
and Bundler declaration checks.

Default CI is offline and deterministic after dependency installation. Hosted provider,
database, browser, OS and long-running resource tests belong to explicit manual lanes.
Local investigations stay in ignored `research/` and are never required by CI.

## Architecture and support evidence

- [Architecture decisions](architecture/README.md)
- [Feature catalog](docs/features.json): 47 stable feature IDs and delivery ownership.
- [Provider catalog](docs/providers.json): 16 distinct targets, configuration-specific operations and evidence.
- [Catalog conventions](docs/catalogs.md)
- [Portable API](docs/core.md), [memory/testing tools](docs/testing.md), [S3 adapter](docs/s3.md), and [managed transfers](docs/transfers.md), and [durable sessions](docs/sessions.md)
- [Native R2 Workers](docs/r2-workers.md) and [local binding evidence](docs/evidence/phase-6-r2-workers.md)
- [Filesystem provider](docs/filesystem.md) and [filesystem evidence](docs/evidence/phase-6-filesystem.md)
- [Runtime setup](docs/runtimes.md) and [current runtime evidence](docs/evidence/pre-phase-6.md)
- [Effect 4.1 baseline upgrade evidence](docs/evidence/effect-4-1.md)
- [Phase 5 persistence/restart evidence](docs/evidence/phase-5.md)
- [Phase 4 transfer/resource evidence](docs/evidence/phase-4.md)
- [Phase 1 evidence](docs/evidence/phase-1.md), [phase 2 evidence](docs/evidence/phase-2.md), and [phase 3 evidence](docs/evidence/phase-3.md)

A documented native capability, local fixture result and real-provider acceptance are
different evidence. Unknown support is never treated as a pass.

## Contributing and releases

Follow [AGENTS.md](AGENTS.md), package ownership and installed Effect documentation.
Keep behavior, tests, public exports and documentation synchronized. Use Conventional
Commits and Changesets for user-visible package changes. Keep shared workflow callers
on `effect-kit/.github@v1`.

Read [.github/README.md](.github/README.md) for release requirements.
`release:check` and `release:smoke` remain unimplemented template placeholders; they
must be replaced before any release or dry-run claim. Artifact tests do not qualify
publishing permissions, exact release plans or provenance. No package has been published
by this work.

## License

Apache-2.0. Each published package includes its own LICENSE.

Native provider guides: [Google Cloud Storage](docs/gcs.md), [Azure Blob](docs/azure.md),
and [phase 7 evidence](docs/evidence/phase-7.md).

Supabase and Vercel adapters: [Supabase](docs/supabase.md), [Vercel Blob](docs/vercel.md),
[runnable examples](docs/app-storage-examples.md), and [phase 8 evidence](docs/evidence/phase-8.md).
Use `pnpm test:app:local` for packed browser module probes and tus process restarts;
`pnpm test:app:hosted` requires explicit existing provider resources and test authority.
