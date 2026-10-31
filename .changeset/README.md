# Releases

The pending initial minor Changeset moves `@effect-kit/storage` from `0.0.0` to `0.1.0`.
Keep that single entry until the first version PR; initial fixes belong to the same release.
After the first release, add Changesets for user-visible package changes. Tooling-only
changes do not need release notes.

Dispatch **Release** on the default branch with `dry-run: true` to verify temporary
versions and exact archives without opening a PR or publishing. When qualification is
complete, dispatch with `dry-run: false` to create or update the version PR. Review and
merge it, then dispatch again to publish using the existing trusted publisher.

`release:prepare` checks source and builds. `release:check --pack-dir DIR` verifies every
package in the supplied `publish-plan.json`, its exact archive and isolated consumers,
and writes `verification.json`. It never modifies the plan or tarballs.

After publication, dispatch **Release smoke** with the successful release run ID and
its packed artifact ID. Smoke compares exact registry bytes, cryptographic signatures,
provenance and installed consumers. It never rebuilds, repacks or publishes.

Provider and independent-review qualification remains separate from archive verification.
Read the root README and `.github/README.md` for commands and workflow contracts.
