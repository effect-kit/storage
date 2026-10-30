# Vercel Blob

`/vercel` provides Storage, native multipart and a transfer driver using the optional
`@vercel/blob` peer. The workspace probes SDK 2.8.2. Node local SDK/HTTP evidence is
available; hosted public/private stores and OIDC remain separate acceptance gates.

```ts
import { Redacted } from "effect";

import { VercelStorage } from "@effect-kit/storage/vercel";

const provider = VercelStorage.layer({
  storeId: "store_example",
  access: "private",
  credential: { mode: "token", token: Redacted.make(readWriteToken) },
});
```

The application must set `VERCEL_BLOB_RETRIES=0` before constructing this Layer.
The pinned SDK reads this switch on every control request; the adapter checks it
before use and never changes process-wide policy. This prevents hidden SDK retries
from replaying streams or escaping managed attempt limits. SDK API-version overrides other than 13 are rejected. SDK upgrades must rerun
these probes; a future public per-client retry option can replace the switch.

`credential.mode: "oidc"` accepts a supplied OIDC token instead. Both modes accept
an Effect returning a Redacted token and resolve it per operation. Explicit token
store identity must match storeId. The namespace includes access and store identity.
No static token/OIDC fallback is selected by the library. Native server imports are
Node-qualified; runtime helper support does not certify SDK transports on Bun/Deno.

## Objects, identity and caching

Primitives are put, scoped streaming get, head, single-object delete and cursor-based
listPage. Exact paths set addRandomSuffix=false and explicit overwrite behavior.
ETags remain validators, not checksums. URL construction encodes path segments;
caller-selected hosts never receive credentials. Empty/dot path segments and keys
that would be interpreted as URLs are rejected. No version history is claimed.

Content type is supported. Other ObjectHeaders fields and application custom metadata
are rejected before transfer source reads; use immutable application keys and keep
richer metadata in the application. The engine's internal operation marker is not
persisted by Vercel, so pathname alone never proves which operation committed.
This adapter does not claim the reference suite's flat custom-metadata round-trip.

Read validators and mutation ifMatch/overwrite controls are mapped explicitly.
304 has no body. The SDK reports ranged 206 as statusCode 200; the adapter verifies
Content-Range and exposes the full size separately from returned byte length.
Conditional deletes are single-object calls because the SDK's validator does not
apply to array deletes. Read errors are translated without exposing SDK messages. Encoded representations
are rejected because Fetch would otherwise transparently decompress stored bytes.

Private reads request useCache=false. The SDK only adds the origin bypass for
private access; public delivery can remain cached. Pinned verification requires the
observed response validator to match the selected identity and fails on stale data.
Deleting an origin object does not revoke previously cached public copies. Public
URLs require an explicit public store and VercelStorage.publicUrls().

Application upload ceilings default to 1 GiB; unknown-length primitive staging defaults
to 8 MiB. Known-length streams are backpressured and preserve source failures.
Single managed uploads are limited to 256 MiB. These are adapter execution limits,
not a promise of any account's service entitlement. Configure lower actual limits.
requestTimeoutMillis defaults to 30 seconds; Transfers adds its shared operation
memory, requests, deadline and attempt limits.

## Multipart and recovery

The adapter uses public createMultipartUpload/uploadPart/completeMultipartUpload
methods. Full parts are at least 5 MiB, maximum admitted part size is 256 MiB,
and the planner caps part count at 10,000. Parts may run concurrently; completion
receipts retain part order. The adapter does not use SDK managed multipart buffering.

Conditional multipart completion is disabled. Public inventory and remote abort are
not established by the reviewed SDK: listParts/abort return Unsupported. No durable
handle codec is supplied, so UploadSessions.create fails before initiating multipart.
An unknown final response remains unknown; finding an object at the pathname is not
reconciliation. A failed/cancelled in-process transfer can leave provider orphan
parts. Cleanup diagnostics report that debt; the application must follow provider
expiry/support procedures, and the library never deletes a committed blob as abort.
Use Supabase tus, S3, GCS or Azure when crash-resumable upload is a requirement.
Durable download support still depends on a pinned validator and qualified ranges.

## Signing and client upload boundaries

`VercelSigning.layer()` from `/vercel/signing` provides operation-scoped GET/HEAD/PUT/
DELETE StorageSigning. VercelUploadGrants.create requires an exact key, expiry and
a positive maximumSizeInBytes, with optional nonempty allowedContentTypes/allowOverwrite. Constraints
are included in delegation issuance and URL signing; returned expiry is capped by
the issued delegation. Wildcard and URL-reserved signing keys are rejected to avoid
broad or misencoded grants. Server SDK maximumSizeInBytes checks are not substituted
for provider-enforced grant constraints.

`/vercel/client` exposes VercelClient.upload for the native browser SDK flow. It takes
a File/Blob and an application handleUploadUrl, with optional protected actor headers.
It contains no store credential configuration. Native SDK client retries/buffering
are not certified as managed TransferPolicy behavior; the full managed browser API
and CORS acceptance belong to phase 11.

`/vercel/uploads` creates an HTTP runtime boundary with VercelUploadHandler.make.
Provide VercelStorage, an application Effect KeyValueStore, namespace, explicit
exclusive ownership, callbackUrl, authorize and onObserved effects. This token-based
handler requires read/write token authority; OIDC applications can use the signing
surface and later presigned handler integration. The owning scope must outlive it.

Requests are bounded to 64 KiB and schema-validated. authorize must check the current
actor and returns provider token constraints. SDK signature verification precedes any
callback effects. Completion checks current backend metadata/ETag before onObserved;
it is not file-content/MIME acceptance. Receipts suppress repeated successful delivery
under one exclusive owner. onObserved receives an idempotencyKey for the application's
transaction: a crash after the effect but before receipt persistence can redeliver.
Only an explicitly authorized server tokenPayload is forwarded; clientPayload does
not become callback authority by default. The application owns receipt retention and must not run another owner concurrently.
Callback payload/clientPayload is not trusted application authorization.

`VERCEL_BLOB_API_URL`/NEXT_PUBLIC_VERCEL_BLOB_API_URL are captured SDK emulator
configuration. Credential-bearing calls fail if the endpoint changes after Layer
construction. Local emulators are protocol evidence, not hosted-store qualification.

See [phase 8 evidence](evidence/phase-8.md), the
[official SDK](https://vercel.com/docs/vercel-blob/using-blob-sdk) and
[client upload authorization](https://vercel.com/docs/vercel-blob/client-upload).
