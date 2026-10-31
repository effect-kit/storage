# Repository automation

Shared CI/release callers use `effect-kit/.github@v1`, following the Redis repository.

- `ci.yml`: fast offline checks on pushes, PRs and merge queues. Automated Changesets
  version PRs follow the Redis convention: check after merge and before publication.
- `extended.yml`: manual artifacts/hardening and selected owned local suites. Runtime
  and OS are inputs; Docker suites require Ubuntu. No hosted credentials.
- `providers.yml`: manual hosted tests on the default branch, using the repository
  `STORAGE_PROVIDERS_ENV` secret containing dotenv content
  matching `.env.example`; only the provider step receives it. No blanket secrets inheritance.
- `release.yml`: shared source preparation, exact-plan packing/verification and authorized
  publication. Dispatch only when release qualification gates are closed.
- `release-smoke.yml`: checks a selected successful release's retained archives against npm.
- PR-title and Changeset-status workflows retain repository metadata checks.

Commands own behavior; workflows only select when/where they run. Root package scripts
accept suite/provider arguments instead of one command or workflow per provider.

`release:prepare` runs source checks and builds. `release:check --pack-dir DIR` reads
`publish-plan.json` and the supplied `packages/*.tgz`, checks every planned package,
runs isolated consumers and writes `verification.json`. Inputs remain unchanged.
`release:smoke --pack-dir DIR --provenance` compares npm bytes, verifies npm signatures
and provenance against this repository/workflow/commit, and runs installed consumers.
It never builds, repacks, versions or publishes. These scripts support the shared
Changesets v1 plan contract and require the normal frozen workspace install.

Automatic CI never discovers `.env` files or runs hosted tests. Provider reports contain
case names, runtime/build identity and cleanup status, not credentials or SDK diagnostics.
Live portable/signing evidence does not qualify retention policies, every native protocol,
all authority profiles, or independent cryptographic audits. Those gates remain in research.

The hosted runner also accepts `--suite release` for native transfers, injected lost-part
responses, condition races, denied credentials/expired grants, cancellation where available
and sequential fresh-process session recovery. `--suite browser` uses Chromium at
`http://127.0.0.1:43179`; resource owners configure CORS separately. Native R2 Workers
currently accepts only `portable`. Tests never update hosted policy or deploy fixtures.
