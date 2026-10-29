# 001 — Package boundaries and provider transports

Date: 2026-10-28. Decision: accepted for implementation, subject to new evidence.

## Decision

Start with one ESM library, `@effect-kit/storage`, and focused subpaths as their
implementations arrive. Core depends on Effect. Providers depend on core and their
transport; transfers depend on narrow capabilities; sessions depend on optional
persistence; HTTP/client bindings depend on public contracts. No global provider
registry, detached runtime or application framework is required.

Core exports the portable service/contracts. Implemented subpaths are testing, s3,
s3/signing and s3/post. Future boundaries include r2/r2-workers, gcs, azure,
filesystem, named S3 profiles, supabase, vercel, transfers, uploads, http and client.
Administrative authority is separate.
Optional peers are declared only when the owning implementation exists.

Use AWS SDK v3 low-level commands for AWS and the selected S3 profiles, including
MinIO and Tigris; native GCS/Azure/Vercel SDKs; native R2 binding; Effect
FileSystem/state for local/testing. Supabase's selected native path is Effect
HttpClient REST: its focused SDK stream result lacks same-response metadata.
Retain explicit RLS/JWT versus privileged paths. Evaluate upstream tus with its
Node Buffer versus browser Blob boundary and disabled/coordinated internal retries.

## Evidence and alternatives

[Local SDK probes](../../docs/evidence/phase-1.md) demonstrate stream/metadata and
public protocol surfaces. They do not qualify hosted services or all runtimes.
Use SDK credential/signature/XML implementations rather than reproducing them.
An extra Effect AWS wrapper is unnecessary for the chosen narrow adapter and
retry/resource ownership; its current peer metadata does allow Effect 4.

Native SDK choice is not unconditional: GCS generation coercion needs a lossless
path/guard, and Vercel range status needs Content-Range-aware handling. Focused
authenticated HTTP supplements are allowed when supported SDK APIs cannot express
the contract. No private SDK method becomes a public compatibility dependency.

## Consequences

SDKs stay out of core/browser imports; constructors expose explicit client ownership.
Keep the existing pinned toolchain. Introduce CLI/React packages only for actual
independent dependency/runtime needs. A raw-client escape hatch carries native
ownership and does not count as an implemented portable capability.

The [S3 implementation](../../docs/s3.md) pins official SDK 3.1159.0, preserves
WHEN_SUPPORTED data-plane checksums and uses one transport attempt. Unknown-length
single-request sources stage under an explicit adapter limit; managed streaming
strategies and total byte/retry budgets remain phase 4. Node built-ins are external
only in the isolated S3 graph; SDK-free packed core/Bundler checks remain required.
