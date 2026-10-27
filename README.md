# EffectKit package template

A starting point for the next EffectKit package. pnpm workspaces, Turborepo, Effect 4,
TypeScript 7, and Klarity configs, with the same shared workflows as Redis and emails.

The example `@effect-kit/package-name` package includes a small Effect API, a test,
an ESM build, and declarations. Replace it with your package's implementation.

## Start a new project

1. Copy this folder into a new project, excluding `.git`, `node_modules`, `.turbo`,
   `.artifacts`, and generated `dist` folders. This folder can stay local; only the
   new package repository needs to be pushed when you want GitHub Actions to run.
2. Rename `packages/package-name` to your package's name.
3. Replace these values in tracked files:

   | Starter value                       | Replace with                                            |
   | ----------------------------------- | ------------------------------------------------------- |
   | `effect-kit-package-name-workspace` | The root workspace name                                 |
   | `@effect-kit/package-name`          | The npm package name                                    |
   | `packages/package-name`             | The package directory, including `repository.directory` |
   | `github.com/effect-kit/template`    | The new repository URL in both manifests                |

   Adjust the scope, author, homepage, issue URL,
   descriptions, and license attribution if the new project has different owners.

4. Replace this README and `packages/<name>/README.md` with the package description,
   installation instructions, API examples, and contribution notes. Replace the
   `greet` example and its test. Keep the relevant script and release instructions.
5. Install the Node version in `.node-version` and pnpm from `packageManager`, then run:

   ```sh
   git init -b main # only when starting from a folder copy without Git history
   pnpm install
   pnpm format
   pnpm ci:check
   pnpm test:extended
   ```

   Commit the updated `pnpm-lock.yaml`. Installation also installs Lefthook when
   Git is initialized; run `pnpm prepare` if you initialized Git afterward.

6. Push to the new repository. Confirm the default branch is `main`, or update
   `.changeset/config.json` and the CI caller. The shared workflows already point to
   `effect-kit/.github@v1`; there is no package path to rename inside them.

Find remaining placeholders before publishing:

```sh
rg 'package-name|github.com/effect-kit/template' --glob '!pnpm-lock.yaml'
```

`research/` exists locally and is fully gitignored. Git does not copy empty or ignored
folders into a fresh clone, so create it with `mkdir -p research` when needed.

## Workspace layout

```text
packages/package-name/   Publishable library, package README, source, and tests
.changeset/             Release notes and versioning configuration
.github/workflows/      Shared workflow callers and manual extended tests
research/               Ignored local experiments and investigation notes
```

Versions live in the catalog in `pnpm-workspace.yaml`. Effect is installed at the root
and in the example package for development; consumers supply Effect through its peer
dependency. Keep that peer range aligned with the catalog when upgrading Effect.
Klarity supplies Oxfmt, Oxlint, TypeScript, tsdown, Vitest, Lefthook, and Commitlint presets.

## Commands

| Command                                                  | Purpose                                                |
| -------------------------------------------------------- | ------------------------------------------------------ |
| `pnpm dev`                                               | Watch library builds                                   |
| `pnpm build` / `pnpm clean`                              | Build or remove package output                         |
| `pnpm format` / `pnpm format:check`                      | Format files or check formatting                       |
| `pnpm lint` / `pnpm lint:fix`                            | Check code or apply lint fixes                         |
| `pnpm typecheck`                                         | Check source, tests, root tooling, and package configs |
| `pnpm test` / `pnpm test:watch`                          | Run deterministic tests or watch package tests         |
| `pnpm check` / `pnpm ci:check`                           | Fast formatting, lint, types, tests, and build         |
| `pnpm check:package`                                     | Check built package metadata and declarations          |
| `pnpm test:artifacts`                                    | Placeholder for package-specific artifact checks       |
| `pnpm test:extended`                                     | Manual suite for artifact checks; add slow suites here |
| `pnpm verify`                                            | Fast checks plus extended checks                       |
| `pnpm changeset`                                         | Add a release note and version bump                    |
| `pnpm release:version`                                   | Apply changesets, refresh the lockfile, and format     |
| `pnpm release:prepare`                                   | Release-specific lint, types, formatting, and build    |
| `pnpm release:pack`                                      | Pack candidates into `.artifacts/release`              |
| `pnpm release:check --pack-dir <directory>`              | Placeholder for packed release verification            |
| `pnpm release:smoke --pack-dir <directory> --provenance` | Placeholder for published release verification         |

`release:prepare` is independent of `ci:check`, so you can tune release source checks
without changing PR checks. `test:artifacts`, `release:check`, and `release:smoke`
currently print "Not implemented". Each new package implements these checks for its API.

Before running releases, implement `release:check` to accept `--pack-dir`, verify every
package in the supplied plan, and write `<pack-dir>/verification.json` without changing
the plan or archives. The shared release workflow requires that report, so the echo
placeholder cannot complete a release or dry run. Implement `release:smoke` to accept
`--pack-dir` and `--provenance` and verify the published packages. It must never publish.

Local packing needs an unpublished candidate version; after a release, add a changeset and
apply versioning on a temporary branch before testing the next candidate.

Pre-commit formats and lints staged files. Commit messages use Conventional Commits.
Pre-push runs `ci:check`. Keep Docker, network services, credentials, fuzzing, and large
compatibility matrices out of `test`, `ci:check`, and `release:prepare`; put them in
`test:extended` or separate manually dispatched workflows.

## GitHub workflows

- **CI** runs the fast gate on PRs, pushes to `main`, merge queues, and manual dispatch.
- **PR title** checks Conventional Commit titles through the shared semantic PR action.
- **Changeset status** adds a read-only release-note summary to the job output.
- **Release** is manual, with verification-only mode enabled by default.
- **Release smoke** is manual and checks an already published release's retained artifact.
- **Extended tests** is manual and runs the fast gate plus extended checks on the pinned
  Node version and the latest Node 24 patch.

GitHub Actions dependency updates are grouped weekly. Add package-specific live or
provider workflows only when the package needs them.

Read the [workflow guide](.github/README.md) for each file's triggers, commands,
permissions, customization points, release implementation requirements, and examples
of writing new workflows. It is included locally with the template.

## Set up publishing

Replace the release verification placeholders described above before using these workflows.

1. Enable GitHub Actions and allow Actions to create pull requests in repository settings.
   Require the CI and PR title checks in your branch rules if desired.
2. Make the npm package available to the publishing account or organization. Configure
   an npm trusted publisher for the new owner/repository and `.github/workflows/release.yml`.
   Because publishing runs in a reusable workflow, follow the current Changesets/npm
   guidance for that setup. Confirm the trusted identity matches before a real release.
   A new package may require an initial publication before npm exposes trusted publisher
   settings; perform that setup deliberately with the package owner.
3. Keep the public package's `publishConfig`, repository metadata, and Apache license
   correct. The root workspace stays private. No npm token is stored in this template.
4. Before the first release, run `pnpm changeset` and select a minor bump to move
   the package from `0.0.0` to `0.1.0`. Commit that release note with your package changes.
   Dispatch **Release** on `main` with `dry-run: true`. It applies temporary versions,
   builds, packs, and verifies archives without creating a PR or publishing.
5. Dispatch with `dry-run: false` to open the Changesets version PR. Review and merge it,
   then dispatch again with `dry-run: false` to publish the verified archives with provenance.
6. Use the successful publishing run ID and packed artifact ID from its job summary to
   dispatch **Release smoke**. A dry-run archive has not been published and cannot pass
   the registry smoke check.

Local verification and dry runs do not prove npm publishing permissions are configured.
See the [shared workflow guide](https://github.com/effect-kit/.github/blob/main/docs/workflows.md)
for the release contract and caller configuration.

## Add another package

Copy `packages/package-name` under `packages/<new-name>`, give it a unique npm name,
and update its description, README, repository directory, and exports. Use `catalog:`
for development dependencies and `workspace:^` for dependencies on other workspace
packages. Add a changeset and run `pnpm install` to refresh the lockfile.

Root build, type checking, tests, and Changesets commands discover all workspace packages.
Implement artifact and release checks for the new repository, covering all packages in
the packed plan. Add representative consumer tests as the public API grows and keep each
package's `files` field narrow.

## License

Apache-2.0. Each published package carries its own copy of `LICENSE`.
