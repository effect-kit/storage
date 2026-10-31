# Working in this repository

This is a pnpm/Turborepo workspace for Effect libraries. Read the root and owning
package README before changing code. The root is private; libraries live in `packages/*`.
Read the root README and relevant `.github/workflows/` files when changing workflows
or implementing release checks.

## Implementation

- Inspect the working tree and relevant code before editing. Preserve unrelated work.
- Make focused changes and use Conventional Commits. Commit each coherent change unless
  the user asks you not to commit. Never push or publish without authorization.
- Prefer simple functions and focused modules. Extract shared code when it has real reuse
  or makes the behavior easier to understand. Avoid unrelated refactors and dependencies.
- Read the installed `node_modules/effect/AGENTS.md` before writing Effect code. Use the
  installed Effect 4 source and documentation to confirm APIs.
- Keep Effect execution at runtime boundaries. Model expected failures explicitly and
  validate untrusted inputs. Preserve public behavior unless the task changes it.
- Keep source, tests and package exports synchronized. Product docs and examples live
  in the separate landing-page repository; do not add docs, examples or architecture
  folders here. Design notes belong in ignored `/research/`. Keep test fixtures in tests.
- Keep local investigations in `/research/`. It is ignored; never force-add it.

## Tooling

- Use the Node version in `.node-version` and pnpm from the root `packageManager`.
- Manage aligned versions in `pnpm-workspace.yaml`; commit lockfile changes.
- Reuse Klarity presets instead of adding another formatter, linter, or test framework.
- Build libraries with tsdown. Publish ESM and declarations from `dist`; keep development
  source conditions out of published exports. Keep `files` narrow and include `LICENSE`.
- Check source and test types. Add meaningful tests for behavior changes and regressions.
  Use `@effect/vitest` for Effect programs and ordinary tests for tooling and pure code.

## Checks and releases

- Run `pnpm format` and `pnpm ci:check` after meaningful changes. Run
  `pnpm test:extended` when changing packaging, exports, declarations, or release tooling.
- Keep PR checks fast and deterministic. Long suites, live services, fuzzing, and provider
  matrices belong in manually dispatched workflows. Add those commands only when needed.
- `test:artifacts` checks packed consumers, including actual README snippets.
  `release:prepare` owns release source checks and builds; `release:check` verifies
  exact archives and writes `verification.json` without changing plans or tarballs.
  `release:smoke` checks retained archives against registry bytes and provenance.
- Release tooling must handle all publishable packages in the plan. Registry smoke checks
  compare downloaded bytes and provenance; they must never publish or rebuild a release.
- Add changesets for user-visible package changes. Do not add release notes for tooling
  changes alone. Keep shared workflow callers on `effect-kit/.github@v1` unless a deliberate
  migration requires another version.
- Report what changed, checks run, skipped checks and reasons, and any remaining limitations.
