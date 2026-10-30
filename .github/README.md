# Workflows for Effect Storage

This repository calls the shared Effect Kit workflows. Package scripts own checks and
artifact behavior; workflow YAML selects when and where those commands run.

Most files call reusable workflows from `effect-kit/.github@v1`. This keeps setup and
release behavior consistent with Redis and emails. Each package owns its root scripts
and the tests those scripts run.

## What each file does

| File                             | When it runs                                         | What it does                                                    | Customize                                               |
| -------------------------------- | ---------------------------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------- |
| `workflows/ci.yml`               | PRs, pushes to `main`, merge queues, manual dispatch | Runs `pnpm ci:check` through shared CI                          | Branch, timeout, optional Bun and reports               |
| `workflows/pr-title.yml`         | PR opened, edited, updated, reopened, or ready       | Checks Conventional Commit PR titles                            | Usually keep as supplied                                |
| `workflows/changeset-status.yml` | The same PR activity                                 | Shows release impact in the job summary                         | Usually keep as supplied                                |
| `workflows/release.yml`          | Manual dispatch from the default branch              | Versions or publishes packages; defaults to a dry run           | Implement release checks, then configure npm            |
| `workflows/release-smoke.yml`    | Manual dispatch                                      | Verifies an already published release using its saved archive   | Implement `release:smoke`                               |
| `workflows/extended.yml`         | Manual dispatch                                      | Runs fast and extended checks on pinned Node and latest Node 24 | Extend real artifact tests; adjust the matrix as needed |
| `dependabot.yml`                 | Weekly                                               | Groups GitHub Actions dependency update PRs                     | Update cadence if needed                                |
| `pull_request_template.md`       | When writing a PR                                    | Prompts for the change and verification                         | Add package-specific reviewer context                   |

CI, PR metadata checks, and package builds are wired up. `test:artifacts` builds,
packs and checks isolated production consumers. `release:check` and `release:smoke`
remain unimplemented placeholders; artifact tests do not verify exact release
plans, publishing permission or registry provenance.

## CI: keep the automatic checks fast

`ci.yml` calls the shared workflow, which installs the workspace with a frozen lockfile
and runs the root `ci:check` command. The template uses:

```json
{
  "ci:check": "pnpm check",
  "check": "pnpm format:check && pnpm lint && pnpm typecheck && pnpm test && pnpm build"
}
```

Keep `test` deterministic and offline. It should not need Docker, credentials, provider
accounts, or a large compiler matrix. Source and test types both belong in `typecheck`.
The caller gives the job ten minutes, and newer commits cancel stale PR CI runs.

For a different default branch, change `push.branches` in `ci.yml` and `baseBranch` in
`.changeset/config.json`. Keep `merge_group` if the repository uses a merge queue.
Avoid path filters on a workflow you intend to require for merging.

Set optional inputs under the calling job's `with`:

```yaml
jobs:
  check:
    uses: effect-kit/.github/.github/workflows/ci.yml@v1
    with:
      timeout-minutes: 10
      bun: true
      report-paths: |
        packages/*/test-results/**
```

Enable Bun only after adding `.bun-version`. Add report paths only after configuring
the tests to produce those files. Node normally comes from `.node-version`; a
`node-version` input overrides it when a workflow needs a matrix.

## PR metadata: titles and release notes

Use titles such as `feat: add a client`, `fix: handle an empty response`, or
`docs: clarify installation`. The title workflow uses the shared semantic PR action.
It runs on `pull_request_target` and reads PR metadata without checking out or executing
PR code. Keep package builds and tests in ordinary `pull_request` CI.

Changeset status is informational. Run `pnpm changeset` for user-visible package changes;
documentation and tooling changes do not need a release note. Its result appears in
the workflow job summary, rather than a bot comment.

## Release: implement the package checks first

The shared release workflow runs these root commands:

| Command                                             | Template state   | Required behavior                                    |
| --------------------------------------------------- | ---------------- | ---------------------------------------------------- |
| `release:version`                                   | Implemented      | Apply Changesets, update the lockfile, and format    |
| `release:prepare`                                   | Implemented      | Run release source checks and build the packages     |
| `release:check --pack-dir <directory>`              | Echo placeholder | Verify the exact packed plan and archives            |
| `release:smoke --pack-dir <directory> --provenance` | Echo placeholder | Verify the published packages against those archives |

`release:pack` is a local Changesets convenience command. The shared release workflow
performs its own packing and saves the artifact that is later published.

Implement `release:check` in the consuming repository using the package's test tools.
It must accept the appended `--pack-dir` argument, read `publish-plan.json`, and handle
every publishable package in the plan. Verify archive integrity, expected contents,
exports, declarations, and a focused isolated consumer. Exit nonzero on failure and
write `<pack-dir>/verification.json` with the packages, versions, integrity values,
and checks actually completed. Leave the input plan and archives unchanged.

The shared workflow requires that report and checks the original file checksums.
The echo placeholder cannot complete a release or dry run. Implement real verification
before using this workflow; do not create an empty report to make the job pass.

Keep these checks focused. Put full provider/compiler matrices and repeated integration
tests in `test:extended`. Change `release:prepare` independently of `ci:check` when the
package needs a different source gate for releases.

After implementing checks and configuring npm as described in the root README:

1. Add a Changeset and push it to the new repository. For the first release, use a
   minor bump from `0.0.0` to `0.1.0`.
2. Dispatch **Release** on the default branch with `dry-run: true`. Temporary versions,
   preparation, packing, and verification run without opening a PR or publishing.
3. Dispatch with `dry-run: false` to create or update the version PR. Review and merge it.
4. Dispatch again with `dry-run: false` to publish the verified archives with provenance.
5. Save the run ID and packed artifact ID from the preparation job summary for smoke checks.

Keep the filename `release.yml`: the shared smoke workflow validates it and npm trusted
publishing uses the calling workflow identity. The shared release workflow serializes
releases; do not add the same concurrency group in the caller. Only the release caller
grants write and OIDC permissions.

## Release smoke: check the published version

Implement `release:smoke` to read the supplied packed plan, fetch each exact published
version from npm, and compare its integrity and downloaded bytes with the saved archive.
Honor `--provenance`, run a focused consumer check, and fail on missing or mismatched
packages. It must not rebuild, version, or publish anything.

Dispatch **Release smoke** with the successful publishing run ID and its packed artifact
ID. The shared workflow checks out that release's original commit, rather than the newest
source, and downloads the retained archive. Its caller has a fifteen-minute timeout.
The `fromJSON` expressions convert dispatch IDs to the numeric inputs required by the
shared workflow; retain those conversions.

A dry-run version was never published, so it is not a smoke candidate. Wait for npm
propagation before retrying a missing version. Packed artifacts must still be available;
the shared workflow retains them for thirty days.

## Extended tests: add slow suites here

`extended.yml` is deliberately manual. Each matrix job installs dependencies, runs
`ci:check`, then runs `test:extended`. The empty Node override reads `.node-version`;
the other job uses `24.x`. This avoids repeating the pinned patch version in YAML.

`test:artifacts` now performs real archive and consumer checks. Extend
`test:extended` with applicable suites as they are implemented, for example:

```json
{
  "test:extended": "pnpm test:artifacts && pnpm test:compat && pnpm test:fuzz"
}
```

Add those package scripts before using this example. Keep live/provider tests in their
own manual workflow when they need credentials or services; the artifact workflow should
remain runnable without provider accounts.

## Write another workflow

Put package commands in `package.json` first and verify them locally. YAML should choose
when and where to run them, rather than duplicate test logic or embed long shell programs.

A reusable workflow is called with `jobs.<name>.uses`. It owns its runner and steps;
configure its supported inputs through `with`. For package-specific setup or services,
write a normal job with `runs-on` and `steps`, and reuse the workspace setup action.
See [GitHub's workflow reuse documentation](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows).

For example, after implementing `pnpm test:live`, create a manual `live.yml`:

```yaml
name: Live tests

on:
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: live-${{ github.ref }}
  cancel-in-progress: false

jobs:
  test:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - name: Check out the selected commit
        uses: actions/checkout@v7
        with:
          ref: ${{ github.sha }}
          persist-credentials: false
      - name: Install the workspace
        uses: effect-kit/.github/actions/setup@v1
        with:
          cache: pnpm
      - name: Run live tests
        run: pnpm test:live
```

Add services only when the test needs them. For provider credentials, configure repository
or environment secrets and pass the named secrets only to the step that uses them. Do not
use blanket `secrets: inherit` for the existing shared callers. Add artifact uploads when
tests produce useful reports, with `if: always()` so failed runs retain available evidence.

## Check and run workflows

From the new package root, run:

```sh
pnpm format:check
pnpm ci:check
actionlint .github/workflows/*.yml
```

`actionlint` is an optional local tool, not a workspace dependency. It validates YAML and
Actions expressions; it does not exercise GitHub permissions, remote action code, or npm
publishing. Implemented extended and release checks also need their own local verification.

After pushing the new package repository, run workflows through its Actions tab or CLI:

```sh
gh workflow run ci.yml --repo effect-kit/your-package --ref main
gh workflow run extended.yml --repo effect-kit/your-package --ref main
gh workflow run release.yml --repo effect-kit/your-package --ref main -f dry-run=true
```

Replace `effect-kit/your-package` with the new repository. Manual dispatch requires the
workflow file on its default branch; see [GitHub's manual workflow guide](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).
Configure required status checks after the first real CI run, using the job names GitHub
actually reports. A Changesets PR created with `GITHUB_TOKEN` may need a manual CI dispatch
on its branch because token-created PRs do not trigger ordinary PR workflows.

## S3 protocol acceptance

`workflows/s3-local.yml` is a manual lane running `pnpm test:s3:local`. It creates
and removes its own pinned RustFS container with generated disposable credentials.
It needs Docker and no AWS account. The automatic CI and artifact lanes remain
independent of services. The [S3 guide](../docs/s3.md) documents a separate explicit
`test:s3:aws` lane for an authorized existing versioned bucket/prefix. Real AWS
acceptance is pending at the owner's request; local evidence cannot certify it.

The manually dispatched local S3 lane also runs managed native/multipart copy and a
generated 320 MiB transfer with a 32 MiB engine budget. It writes
`.artifacts/transfers-resources.json` alongside the provider report. The manual provider config exposes GC for retained-memory sampling; this is a
resource profile, not a throughput benchmark. These tests are
local-only, do not run in default CI and do not establish hosted AWS qualification.

The same local command builds subprocess fixtures and exercises durable upload/download
recovery across fresh Node processes. `.artifacts/session-restart.json` records the
crash matrix. No SQL service is required. Distributed coordination and hosted AWS
remain outside that local evidence.

## Runtime helper matrix

`workflows/runtimes.yml` is manual. It runs `pnpm test:runtimes <runtime>` separately
for Node, Bun 1.4.4, Deno 2.9.6 and headless Chromium. Each job installs an exact
local archive in an isolated app with only its selected platform peer. Both
NodeNext and Bundler declarations are checked without Node ambient types. Browser
bundling rejects Node/server SDK imports; the actual browser executes the fixture.
Reports are written to `.artifacts/runtime/<runtime>.json`.

This workflow has not been dispatched as part of local development. Local Darwin
runtime results do not establish its Ubuntu/remote success. Chromium installation
and alternate engines stay outside automatic PR CI. See [runtime commands and
limits](../docs/runtimes.md); real provider acceptance remains in its own lanes.

## Phase 6 local provider acceptance

`workflows/phase-6-local.yml` manually runs Node/Bun filesystem acceptance on macOS
and Ubuntu and an isolated native R2 Worker on Ubuntu. It retains only the selected
safe JSON reports for fourteen days. The local filesystem suite uses real temporary
roots, independent writers and killed subprocesses. The Worker suite uses pinned
Miniflare/workerd, an owned disposable bucket and disabled outbound network; no
Cloudflare credentials or remote binding are supplied. Workerd's verified binary
installer is explicitly allowed in pnpm's build policy for this development tool.

Local Darwin results do not certify those remote Ubuntu runners. This workflow was
linted locally but not dispatched. Windows, hosted R2 and physical power-cut tests
remain separate qualification work. The local R2 reset fixture must never be deployed
against application data; the application example is a separate read-only Worker.

`native-local.yml` is a manually dispatched GCS/Azure lane. It runs deterministic
SDK contract checks, an owned loopback Azurite container and nine native process
restart boundaries with encrypted checkpoints. It needs Docker and no cloud
credentials. The pinned emulator explicitly bypasses its API-version check;
report JSON records that limit. Upload only the two whitelisted local reports.
Hosted GCS/Azure commands require owner-provided existing namespaces and credential
setup; no hosted workflow or account provisioning is implicit in this local lane.

`app-storage-local.yml` is manually dispatched. It runs package checks, installs
Chromium, verifies packed Supabase/Vercel browser imports and exercises owned tus
SIGKILL/restarts and runnable examples. It needs no hosted credentials. Only the
whitelisted `.artifacts/app-storage-local.json` summary is uploaded; bundles,
checkpoints, source files and environment values remain local. Hosted tests are a
separate explicit command documented in `docs/app-storage-examples.md`.
