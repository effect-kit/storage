# Google Cloud Storage

`@effect-kit/storage/gcs` uses the official `@google-cloud/storage` 8.4 peer.
The implemented profile runs on Node 24.22+. Hosted qualification is pending;
local tests exercise the official SDK authorization hook and the JSON API protocol.

```ts
import { GcsStorage } from "@effect-kit/storage/gcs";

const provider = GcsStorage.layer({ bucket: "application-bucket", projectId: "application" });
```

Application default, workload and service-account credentials remain SDK owned,
including refresh. Explicit `credentials`, `authClient` and `keyFilename` are
accepted. The service exposes its native client; no credential material is copied
to its public configuration or session checkpoints. The SDK has no client close
method. `makeClient`/`layerClient` borrow a client, require an explicit stable
`storeId`, matching endpoint, and SDK automatic retries disabled with maxRetries 0.
Object request attempts belong to the transfer engine; credential refresh is
separate SDK activity. `requestTimeoutMillis` bounds an HTTP exchange.

The JSON API transport uses the SDK's public authenticated request hook with one
abortable Node HTTP exchange and no redirects. This avoids `File` converting
string generations to unsafe numbers and supports bounded streaming request bodies.
`apiEndpoint` overrides require explicit authentication selection; anonymous local
fixtures set `useAuthWithCustomEndpoint: false`. Hosted suites reject overrides.

| Surface                                                                 | Behavior                                                                             |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `put`, `head`, `get`, `delete`, `listPage`                              | Portable operations, scoped single-use read bodies and query-bound cursors           |
| Portable conditions                                                     | Atomic create-only; ETag conditional reads. ETag mutation conditions are unsupported |
| `putObject(request, preconditions)`                                     | Native generation/metageneration preconditions, expressed as decimal strings         |
| `updateMetadata({ ref, headers, preconditions })`                       | Metadata update with independent generation/metageneration checks                    |
| `deleteGeneration({ ref, preconditions })`                              | Native generation selection and conditional deletion                                 |
| `offsets.create/query/write/cancel`                                     | Native resumable protocol with protected bearer handles                              |
| `rewrite({ source, destinationKey, headers?, preconditions?, token? })` | One rewrite step: Pending token or Completed metadata                                |
| `compose({ sources, destinationKey, headers?, preconditions? })`        | Up to 32 explicitly generation-pinned components                                     |
| `GcsStorage.publicUrls(baseUrl?)`                                       | Explicit public-delivery service; does not change object visibility                  |

Names are bounded to 1024 UTF-8 bytes and exclude the native forbidden `.`/`..`
names. Internal dot segments remain opaque bytes in JSON API paths. Validation runs
before provider I/O and managed source reads.

A generation is immutable object data; metageneration records metadata changes.
Both stay strings through responses, request queries and checkpoint encoding.
ETags use HTTP validator quoting; bare JSON resource tags receive those framing
quotes once, so HEAD receipts and download response headers remain comparable.
Native object mutation preconditions accept `ifGenerationMatch`, `ifGenerationNotMatch`,
`ifMetagenerationMatch`, and `ifMetagenerationNotMatch`. Compose accepts only
its supported positive match conditions; unsupported negative conditions fail
before I/O. Rewrite and compose retain the configured destination KMS key. Pin the generation as
well when checking metageneration. Never turn a metadata lookup followed by an
unconditional write into an advertised atomic condition. JSON API ETag headers
apply to retrieval, so portable ETag conditional delete is not advertised.

Metadata and download bytes come from the same HTTP response. Reads request stored
gzip bytes and disable automatic decompression. Full-object CRC32C is validated
incrementally using the SDK's validator when the response supplies it; range
responses retain full-object checksum observations without claiming a range digest.
Primitive puts compute CRC32C incrementally and compare returned CRC32C. Known
byte inputs also supply the expected checksum in the multipart metadata.
`kmsKeyName` selects a managed encryption key. Customer-supplied encryption keys,
soft-delete recovery and governance are outside this adapter profile.

Primitive puts use one streaming JSON multipart exchange, so a managed single
upload does not hide a resumable initiation request from its attempt budget.
Primitive puts are capped at 256 MiB. Unknown-length primitive streams stage within
`maxUnknownLengthBytes` (default 16 MiB), additionally bounded by `maxBytes`.
Known-length streams use backpressure and preserve source errors through EOF.

For managed uploads, the provider layer supplies the driver automatically.
`strategy: "offset"` selects native resumable uploads; auto selects it when the
known source exceeds the single-buffer threshold. Offset uploads require a known
length, use 256 KiB-aligned nonfinal chunks and query the server before replay.
Partially acknowledged chunks retain only the unaccepted suffix. A lost final
response is probed, never assumed to mean failure. A native managed receipt requires
a generation-pinned SHA-256 readback; budget and account for that additional read.
Use bounded `strategy: "single"` for an unknown-length managed source.

Durable sessions fingerprint a replayable known source, restrict session URLs to the configured bucket, authority and native session route,
protect them in the
production checkpoint store, probe the actual remote offset after restart, and
revalidate the complete destination digest. They do not restore an incompatible
SDK CRC state. Unknown initiation without a saved handle requires orphan discovery;
it is never adopted by object key. Expired sessions fail explicitly. Cancellation
uses the JSON resumable DELETE operation; its successful 499 response is intentional.

Signing is separate: provide `GcsSigning.layer()` from `/gcs/signing`, or
`GcsPost.layer()` from `/gcs/post`, with the provider layer. The SDK signs locally
or through IAM signBlob; data credentials alone need not grant signing. Grants
are Redacted, expire within seven days, and can be capped with `authorityExpiresAt`.
The SDK signing surface supports GET/PUT/DELETE; HEAD is explicitly unsupported.
Generation-specific PUT and KMS-configured PUT signing are rejected. POST policies
bind an exact key and byte range. Public URLs reject versioned refs and dot segments.

`rewrite` returns one continuation at a time. Repeat with the same pinned source,
destination, metadata and conditions; only Completed carries a destination receipt.
Managed copy currently uses the shared streamed-copy path and verifies source and
destination. Native rewrite/compose remain explicit provider operations.

Run `pnpm test:native:local` for local protocols, or configure the documented hosted
lane in [phase 7 evidence](evidence/phase-7.md). Examples:
`packages/storage/examples/gcs.mjs` and `native-signing.mjs`.

See [runnable native examples and environment settings](native-examples.md).
