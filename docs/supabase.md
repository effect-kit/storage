# Supabase Storage

`/supabase` provides native Storage and its transfer driver over Effect HttpClient.
It requires no Supabase SDK, AWS SDK, database client or tus package. Node local
HTTP/protocol tests are implemented; hosted RLS and deployment semantics remain
unqualified. Browser bundling is a separate check from browser/CORS acceptance.

```ts
import { Effect, Redacted } from "effect";

import { Storage } from "@effect-kit/storage";
import { SupabaseStorage } from "@effect-kit/storage/supabase";

const provider = SupabaseStorage.layer({
  url: "https://your-project.supabase.co",
  bucket: "files",
  storeId: "application:files:user-a",
  auth: { mode: "user", token: Redacted.make(userJwt) },
});
const program = Effect.gen(function* () {
  const storage = yield* Storage;
  return yield* storage.put({
    key: "user-a/avatar.png",
    body: bytes,
    contentType: "image/png",
    condition: { _tag: "CreateOnly" },
  });
}).pipe(Effect.provide(provider));
```

`url` is the project or direct storage origin, without `/storage/v1`. Supply the
actual bucket and a stable application namespace identity. Keys containing empty,
`.` or `..` path segments are rejected before transport. Slashes, Unicode and
encoded characters retain their meaning through segment encoding.

## Credentials and transport

Choose `auth.mode: "user"` for a user JWT or `"service"` for privileged authority.
`auth.token` accepts a Redacted string or an Effect returning one; the latter runs
for every native request. An optional Redacted `apiKey` supplies the API-key header.
The mode declares intended authority; it does not downgrade the permissions of a
privileged credential. No request falls back to another authority. Bind each Layer/configurationId to its
actor and policy; do not share a mutable user token source between unrelated users.
Application authorization and RLS policies remain application responsibilities.

The default layer captures FetchHttpClient with redirects rejected and HTTP cache
bypassed. `layerHttp` and `make` accept an application-provided HttpClient service;
that client must preserve abort/body ownership and perform no hidden retries or
credential-bearing redirects. Primitive operations do not retry themselves.
Expected failures contain classified status, never response bodies or credentials.
An upstream 404 is reported as absence; some RLS policies deliberately conceal
objects, so the adapter cannot infer whether an absent-looking object exists.

The REST choice resolves the inspected storage-js 2.118.0 stream builder's lack of
same-response metadata. Downloads retain headers and bytes together; a later info
request never supplies identity for an earlier body. Upload acknowledgements contain
key/size without an invented ETag. `head` uses native info; lists use list-v2 with
query-bound cursors. Native version selectors are not yet qualified as portable
versioned reads. Storage SQL tables are never mutated by this adapter.

## Objects and constraints

Native uploads preserve content type, cache control and flat string custom metadata.
Encoded content and content-disposition/language fields fail before source reads.
Fetch automatically decodes Content-Encoding, so encoded representations are rejected
on reads rather than returning altered storage bytes. Native info permits null fields
and richer custom JSON; only string custom values map to the portable metadata type. Downloads expose observed headers, conditional outcomes and validated ranges.
Read conditions are checked against the same response, including deployments that
ignore request validators. Mutation ETag conditions are unsupported; create-only uses
native non-upsert writes. Listing supports `/` delimiters and is not a snapshot.

`maxObjectBytes` is an application ceiling (default 50 MiB), not discovery of a plan
entitlement. Configure it from the actual project's global and bucket settings.
Unknown-length primitive streams stage up to `maxUnknownLengthBytes` (default 8 MiB).
Known-length streams use a bounded Web bridge and preserve caller source errors.
`requestTimeoutMillis` defaults to 30 seconds for each exchange. Managed Transfers
add shared memory/request/deadline/attempt admission above these primitives.

## tus transfers and protected restart

Set `resumable: true` to negotiate tus 1.0 creation support at Layer acquisition.
That bounded OPTIONS request is setup, outside an individual transfer's attempt
budget. Each subsequent create, HEAD, PATCH, completion info and DELETE is a
separately budgeted request. Supabase requires 6 MiB full chunks; the planner selects
that fixed size automatically, admits its workspace, and allows a smaller final chunk.
Only known-length replayable sources support durable upload recovery.

The existing engine owns chunk buffering, retry decisions, committed-offset queries,
source fingerprints and encrypted checkpoints. The public tus-js-client 4.3.1 probe
confirmed resume, hooks and disabled retries, but its Upload object owns the upload
loop. We selected focused Effect HTTP tus operations to fit the existing per-request
journal/budget without a second scheduler or SDK-owned URL persistence. This is a
narrow tus 1.0 implementation, not a general replacement for tus-js-client.

No checksum, concatenation or deferred-length extension is assumed. The uploaded
operation marker and size must match native info before a completion receipt can be
adopted. The engine then verifies a pinned full SHA-256 readback. A changed source
fails before more provider requests. A lost initiation response cannot be adopted by
key alone; it remains reconciliation/orphan debt. Expired sessions remain explicit.

Only an advertised termination extension permits remote cancellation. Otherwise abort
reports unsupported cleanup and the session remains cleanup-pending until application
reconciliation/provider expiry. Cancellation never deletes an existing object to
simulate cleanup. tus content encoding and cache policies other than `max-age=N` are
rejected instead of ignored.

Compose `Transfers.layer(policy)` and the selected runtime layer as for GCS. Add
`UploadSessions.layer({ configurationId })` and the existing protected Effect KVS
store only when restart is needed. No PostgreSQL integration is required.

## Grants and S3 access

`SupabaseSigning.layer()` from `/supabase/signing` supplies StorageSigning for native
GET URLs and SupabaseUploadGrants.create({ key, upsert? }). Upload grants return a
protected PUT URL/token, provider upsert constraint and expiry decoded from the
issued token (normally two hours); they do not accept a fictitious arbitrary upload lifetime. Signed resumable
clients can use that token as `x-signature` in their own native browser flow. Managed
sessions currently use the configured JWT/service credential, not upload grants.
HEAD/DELETE signing is unsupported on this native surface. URL-reserved signing
keys are rejected until their native token/path encoding is qualified. Grant URLs remain bound
to the configured origin and native signing routes. Public URL resolution is an
explicit `SupabaseStorage.publicUrls()` Layer and never changes bucket visibility.

`/supabase/s3` separately exports SupabaseS3Storage. Install the optional AWS client
peer, provide the exact `/storage/v1/s3` endpoint/region, and explicitly choose
`auth.mode: "server-keys"` or `"user-session"`. Generated server keys bypass RLS;
user-session uses projectRef, anonKey and a refresh callback returning the JWT. JWT
expiry is supplied to the SDK's credential cache. Required-only checksums avoid AWS
checksum defaults; optional checksum reads are disabled on this profile.

That profile exposes primitives and in-process multipart. Portable versions, mutation
conditions, conditional multipart completion and durable S3 sessions remain disabled
pending compatible-server evidence. It is never an automatic native-auth fallback.
AWS transport failures retain the underlying `s3` classification. Advanced native
administration/versioning belongs to phase 10.

See [phase 8 evidence](evidence/phase-8.md), [sessions](sessions.md), the
[Supabase authentication matrix](https://supabase.com/docs/guides/storage/s3/authentication),
and [native tus requirements](https://supabase.com/docs/guides/storage/uploads/resumable-uploads).
