# Effect Kit Storage

Effect-native object storage services and workflows. Under development; not published
by this project work. The library lives in `packages/storage` and targets Effect 4.1.
Provider SDKs, platform services and React bindings use isolated optional entry points.

Use Node from `.node-version` and pnpm from the root `packageManager`.

| Command                                                                         | Purpose                                                  |
| ------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `pnpm install --frozen-lockfile`                                                | Install the pinned workspace                             |
| `pnpm ci:check`                                                                 | Format, lint, source/test types, offline tests and build |
| `pnpm test:extended`                                                            | Exact-package consumers and local hardening              |
| `pnpm test:local --suite react`                                                 | Selected owned local suite                               |
| `pnpm test:local --suite filesystem --runtime bun`                              | Select runtime/OS-sensitive checks                       |
| `pnpm providers:prepare --credentials-file .env.providers.local --provider s3`  | Validate explicit hosted settings without network access |
| `pnpm test:providers --credentials-file .env.providers.local --provider s3,gcs` | Run selected hosted profiles                             |
| `pnpm test:providers --credentials-file .env.providers.local --provider all`    | Require and test all profiles                            |
| `pnpm providers:worker`                                                         | Build/check the native R2 test fixture; does not deploy  |
| `pnpm release:check --pack-dir DIR`                                             | Verify the exact supplied release plan/archives          |
| `pnpm release:smoke --pack-dir DIR --provenance`                                | Verify published bytes, provenance and consumers         |

Copy `.env.example` to `.env.providers.local`, fill only the services you select and
keep that file private. Environment loading is explicit; ordinary CI/tests never read it.
Hosted tests use existing isolated resources and generated keys under the configured
prefix. They do not create buckets or change policies. Cleanup uncertainty fails the run.
Reports live in ignored `.artifacts/`. `--suite signing` additionally checks native signing
and requires its corresponding authority. These bounded suites do not certify every
native provider feature or authority configuration.

Local suite choices: `all`, `s3`, `native`, `app`, `filesystem`, `runtimes`, `r2`, `uploads`,
`composition`, `react`. Runtime choices apply to filesystem/runtime helpers. Fixtures are
internal test assets, not public example applications.

Repository workflow contracts are in `.github/README.md`. Product docs and examples
belong to the separate landing-page repository. Design/planning records stay in ignored
`research/`; they are not required build or CI inputs. No CLI product is planned.

Use Conventional Commits and Changesets for user-visible library changes. Never push,
publish or deploy without authorization. Release readiness requires the qualification
gates recorded in research, including hosted evidence and independent security review.

Apache-2.0. Published packages include LICENSE.
