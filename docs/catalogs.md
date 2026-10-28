# Feature and provider catalogs

Run `pnpm catalog:check`. Schemas live in `scripts/catalog/schema.ts`; cross-record
coverage/reference/evidence checks live in `scripts/catalog/validate.ts` and run
in ordinary CI with their negative tests.

`features.json` preserves 47 stable IDs, complete target text, completion phase,
status, specification/implementation/test/example references and evidence.
Planned or in-progress features must not be presented as shipped. Implemented
features need real source, test and specification references.

`providers.json` preserves all 16 targets. Each includes a proposed transport,
implementation phase, runtime targets and source notes. Runtime targets are plans,
not qualification badges. Core operation cells start research-needed.

Add operation/configuration/runtime rows as concrete profiles are implemented.
Multiple rows for one operation are expected when configuration changes its
semantics. The same tuple cannot repeat. Feature IDs link each cell to the catalog.
Advanced operation coverage grows with implementation; the initial five core cells
are not the complete advanced provider matrix.

Evidence records carry kind/date/source, SDK/API version when applicable, runtime,
configuration, test ID and limitations. No root schema or helper imports the JSON
catalogs at runtime.

| Status                        | Meaning                                                            |
| ----------------------------- | ------------------------------------------------------------------ |
| verified                      | Implemented and tested in the required real provider/runtime scope |
| implemented-not-live-verified | Source/tests exist; hosted semantics remain unverified             |
| unsupported-native            | Native absence supported by documentation/source/hosted evidence   |
| unsupported-adapter           | Native feature may exist; this adapter does not implement it       |
| research-needed               | Behavior or qualification is unresolved                            |
| explicitly-deferred           | Owner-approved destination, rationale and ownership are recorded   |

For verified cloud support, require matching hosted configuration/runtime evidence.
For local adapters, require runtime evidence. Compile and local protocol results
belong in evidence summaries, not a cloud verified cell. References must resolve
to maintained files or valid HTTPS sources; ignored research and escaped paths
are rejected.
