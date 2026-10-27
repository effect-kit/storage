# Release notes

Run `pnpm changeset` for user-visible changes. Select the affected packages and write a short release note. Tooling-only changes do not need a changeset.

The starter has no pending changesets. Before the first release, run `pnpm changeset` and select a minor bump to move the package from `0.0.0` to `0.1.0`.

Implement the `release:check` and `release:smoke` placeholders first. Then dispatch **Release** with `dry-run: true` to verify temporary versions and exact archives. With `dry-run: false`, pending changesets create a version PR; merge it, then dispatch again to publish. See the root README for npm setup.
