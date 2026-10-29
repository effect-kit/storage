# Effect 4.1 baseline upgrade

Date: 2026-10-30. Runtime: Node 24.22.0, Darwin arm64. Effect and
@effect/vitest: 4.1.0. TypeScript: 7.0.2. AWS SDK: 3.1159.0.

The workspace catalog/lockfile now use Effect 4.1.0 and its aligned test integration.
The library's Effect peer minimum is `^4.1.0`. No runtime platform package, provider
SDK or cryptographic dependency was added. Existing API paths and constructor
signatures remain in this upgrade; their pre-release redesign is separate work.

The atomic checkpoint backing implements the newly required `keys` operation with
a typed KeyValueStoreError because its hashed filenames cannot reconstruct logical
keys. Nonempty `clearPrefix` calls fail before removing records. A real-file test
verifies both failures and retained checkpoint contents. Session recovery continues
to use application-known IDs. No key index or new ciphertext format is implied.

Executed checks:

- `pnpm install --frozen-lockfile`: aligned dependency resolution.
- `pnpm format` and `pnpm ci:check`: format, catalog, lint, source/test/tooling types,
  115 package tests, 10 tooling tests and builds.
- `pnpm test:extended`: strict package checks and exact packed ESM, NodeNext/Bundler
  declaration/runtime consumers, including generic SDK-free sessions and explicit
  optional S3/signing/POST peers.
- `pnpm test:s3:local`: ten passes and one AWS-only skip, including the separate-
  process session restart matrix and generated 320 MiB resource case with a
  32 MiB engine admission budget.

Reports remain in `.artifacts/artifact-check.json`, `.artifacts/s3-acceptance.json`,
`.artifacts/session-restart.json` and `.artifacts/transfers-resources.json`.
Command logs are `.artifacts/effect-4-1-*.log`.

No AWS account was accessed. Hosted AWS remains owner-postponed. No new Bun, Deno,
browser, Worker or OS qualification is claimed; no publish/push action occurred.
The peer range permits newer 4.x versions, but those versions require separate
qualification, especially for unstable integration APIs.
