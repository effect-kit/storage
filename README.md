# Effect Storage

**Object storage for Effect v4.** Typed operations, scoped streaming, managed transfers
and resumable workflows across cloud providers and local storage.

The npm package is [`@effect-kit/storage`](https://www.npmjs.com/package/@effect-kit/storage).
Provider SDKs, runtime Layers and React bindings are isolated optional integrations.

```sh
pnpm add @effect-kit/storage effect
```

```ts
import { Effect } from "effect";

import { Storage } from "@effect-kit/storage";
import { MemoryStorage } from "@effect-kit/storage/testing";

const program = Effect.gen(function* () {
  const storage = yield* Storage;

  const saved = yield* storage.put({
    key: "greetings/hello.txt",
    body: new TextEncoder().encode("Hello from Effect"),
    contentType: "text/plain",
  });

  return yield* Storage.getText({ ref: saved.ref, maxBytes: 1024 });
});

const greeting = await Effect.runPromise(program.pipe(Effect.provide(MemoryStorage.layer())));
console.log(greeting);
```

See the **[package README](packages/storage/README.md)** for installation, S3 configuration,
streaming, conditions, transfers, durable sessions, signing and integration boundaries.

## What it provides

- Portable `Storage` service with typed errors, references, ranges and capability checks.
- AWS S3 and named S3-compatible profiles, native R2, GCS, Azure, Supabase and Vercel adapters.
- Memory and filesystem storage, fault injection and reusable adapter conformance checks.
- Managed upload/download/copy/move operations with integrity checks and bounded resources.
- Protected durable upload/download sessions backed by Effect persistence.
- Authorized upload workflows, Effect HttpApi integration, browser clients and React hooks.
- Lifecycle/version/metadata services, separate administration authority and event contracts.
- Composition, Effect Cache, migrations, mirroring, content addressing and representations.

Compatibility is scoped to actual tested operations and configurations. Other hosted
providers are deferred while the configured providers undergo deeper acceptance.
Provider-specific recovery, governance and browser authority require their own evidence.
The optional age encryption integration has not received an independent security audit.

## Development

Use Node **24.22.0** from [.node-version](.node-version) and pnpm **12.14.0** from
`packageManager`. Local runtime checks also use Bun **1.4.4**, Deno **2.9.6** and
Playwright Chromium; Docker is required for owned service fixtures.

```sh
pnpm install --frozen-lockfile
pnpm ci:check
```

The workspace uses pnpm/Turborepo, TypeScript, tsdown and Klarity's formatter, linter
and test presets. Dependency versions are aligned in `pnpm-workspace.yaml`.
Libraries live in `packages/*`; this root package is private.

| Command                                                                                                  | Purpose                                                                |
| -------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `pnpm ci:check`                                                                                          | Format, lint, source/test types, offline tests and build               |
| `pnpm test:extended`                                                                                     | Exact-package consumers, README checks and resource/recovery hardening |
| `pnpm test:local --suite all --runtime all`                                                              | Owned service, filesystem, runtime, browser and React suites           |
| `pnpm test:local --suite react`                                                                          | Select one local suite                                                 |
| `pnpm providers:prepare --credentials-file .env.providers.local --provider s3`                           | Validate selected settings without network access                      |
| `pnpm test:providers --credentials-file .env.providers.local --provider s3,r2 --suite release`           | Deep acceptance for selected providers                                 |
| `pnpm test:providers --credentials-file .env.providers.local --provider supabase,vercel --suite browser` | Real Chromium signed-read/CORS acceptance                              |
| `pnpm providers:worker`                                                                                  | Build and check the native R2 fixture locally; does not deploy         |
| `pnpm release:check --pack-dir DIR`                                                                      | Verify exact supplied release plan and archives                        |
| `pnpm release:smoke --pack-dir DIR --provenance`                                                         | Verify published bytes, provenance and consumers                       |

### Provider acceptance

Copy [.env.example](.env.example) to `.env.providers.local`. Configure only the selected
providers and existing dedicated resources. Keep the file private; credentials are never
loaded automatically by ordinary tests or CI.

```sh
pnpm providers:prepare --credentials-file .env.providers.local --provider s3,r2
pnpm test:providers --credentials-file .env.providers.local --provider s3,r2 --suite release
```

The runner accepts comma-separated provider IDs or `all`; `all` requires every profile
and never silently skips missing credentials. Suites are selected with an argument:

- `portable`: bounded object, range, list, stream and managed-integrity checks.
- `signing`: portable checks plus a real signed GET.
- `release`: signing, condition races/stale writes, credential and expiry rejection,
  native multipart/resumable integrity, checksum mismatch, available cancellation and
  fresh-process recovery. Supabase enables tus for this suite. Unsupported native
  capabilities are recorded explicitly.
- `browser`: signing checks plus a real Chromium fetch from a loopback web origin.
  The provider must allow `http://127.0.0.1:43179` through its existing CORS policy.

Release fixtures use roughly 12 MiB per large object and a bounded number of operations;
provider requests, storage and egress can incur their normal charges. Tests own unique
keys under the configured acceptance prefix. They never create buckets or change policies.
Cleanup deletes exact observed receipts; uncertain outcomes fail the run and remain debt.
Reports contain sanitized results and build identity in ignored `.artifacts/`.

Endpoints require HTTPS. Local MinIO additionally accepts HTTP on `localhost`,
`127.0.0.1` or `[::1]`; bind local server ports to loopback. MinIO edition and server
version are explicit configuration, rather than inferred compatibility promises.

Local suite selectors are `all`, `s3`, `native`, `app`, `filesystem`, `runtimes`, `r2`,
`uploads`, `composition` and `react`. Runtime selectors are `node`, `bun`, `deno`,
`browser` and `all`, where applicable. Internal fixtures are test assets.

## CI and releases

PR/main checks are offline and deterministic. Isolated package consumers prefer cached
dependencies and may fetch missing registry metadata; install lifecycle scripts stay disabled. Extended and hosted acceptance are manual
workflows. Shared workflow callers use `effect-kit/.github@v1`; their configuration lives
in [.github/workflows](.github/workflows).

One initial minor Changeset prepares `@effect-kit/storage` for **0.1.0**. Changesets owns
versions and changelogs. **Release** defaults to a dry run that verifies temporary versions
and exact archives without opening a PR or publishing. With dry run disabled, pending
Changesets create the version PR; after review and merge, a subsequent release dispatch
publishes through the existing trusted-publishing configuration.

`release:prepare` owns source checks and builds. `release:check` checks exact archives,
their hashes, declarations and isolated consumers without changing the plan or tarballs.
**Release smoke** checks retained release bytes against npm signatures/provenance and
installed consumers after publication. See [.changeset/README.md](.changeset/README.md).

Ordinary pushes and PRs do not publish. Remaining external qualification or independent
review is recorded explicitly; successful local checks alone do not close those gates.

## Contributing

Follow [AGENTS.md](AGENTS.md), use Conventional Commits and include meaningful tests for
behavior changes. Keep public APIs, package exports and consumers synchronized. Source
modules are grouped by domain under `packages/storage/src`; tests are organized into
`unit`, `integration`, `e2e` and `fixtures`, with Vitest configs under `test/`.

Product documentation and examples live in the separate landing-page repository.
Local plans and investigations stay in ignored `research/`; builds and CI do not depend
on those files. This repository has no CLI product.

Open [issues](https://github.com/effect-kit/storage/issues) for bugs and proposals.
For vulnerabilities, use GitHub private reporting when available; keep credentials and
exploit details out of public issues.

## License

[Apache-2.0](LICENSE) · Copyright 2026 Effect Kit Contributors.
