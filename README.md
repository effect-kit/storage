# Effect Kit Storage

Effect-native object storage for applications, workers and upload backends.

The package is under development. The portable Storage service provides uploads,
scoped downloads, metadata, deletion and pagination. The `/testing` entry point
provides memory storage, fault scripts and a public adapter conformance suite.
The official-SDK S3 adapter adds native copy, multipart and signing. It has local
protocol evidence; hosted AWS acceptance is pending. Managed transfers follow in
later phases.

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

| Command                        | Purpose                                                     |
| ------------------------------ | ----------------------------------------------------------- |
| `pnpm dev`                     | Watch package builds                                        |
| `pnpm build`                   | Build ESM and declarations with tsdown                      |
| `pnpm format` / `format:check` | Oxfmt formatting                                            |
| `pnpm lint`                    | Oxlint, including tooling                                   |
| `pnpm typecheck`               | Source, tests, tooling and configurations                   |
| `pnpm test`                    | Offline catalog/archive-policy and package tests            |
| `pnpm catalog:check`           | Validate complete feature/provider inventories and evidence |
| `pnpm ci:check`                | Formatting, catalogs, lint, types, tests and build          |
| `pnpm check:package`           | Publint and Are the Types Wrong                             |
| `pnpm test:artifacts`          | Build, pack and test isolated production consumers          |
| `pnpm test:extended`           | Artifact checks; add later manual suites as they exist      |

The artifact lane installs exact local tarballs with Effect and TypeScript from the
local pnpm store in offline mode. A frozen workspace install seeds the required store;
core/testing consumers install no provider peers. A separate S3 consumer installs
its explicit client/signing peers. Consumers run outside the workspace with NodeNext
and Bundler declaration checks.

Default CI is offline and deterministic after dependency installation. Hosted provider,
database, browser, OS and long-running resource tests belong to explicit manual lanes.
Local investigations stay in ignored `research/` and are never required by CI.

## Architecture and support evidence

- [Architecture decisions](architecture/README.md)
- [Feature catalog](docs/features.json): 47 stable feature IDs and delivery ownership.
- [Provider catalog](docs/providers.json): 16 distinct targets, configuration-specific operations and evidence.
- [Catalog conventions](docs/catalogs.md)
- [Portable API](docs/core.md), [memory/testing tools](docs/testing.md), and [S3 adapter](docs/s3.md)
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
