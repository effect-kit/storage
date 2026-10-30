# Azure Blob Storage

`@effect-kit/storage/azure` uses the official `@azure/storage-blob` 12.34 peer.
The implemented profile is Node 24.22+, block blobs, public-cloud account semantics.
Azurite and controlled SDK HTTP tests are local evidence; hosted qualification,
HNS/ADLS, append/page blobs and sovereign clouds remain unqualified.

```ts
import { AzureStorage } from "@effect-kit/storage/azure";
import { defaultCredential } from "@effect-kit/storage/azure/identity";

const provider = AzureStorage.layer({
  containerUrl: "https://account.blob.core.windows.net/uploads",
  credential: defaultCredential(),
});
```

The `/azure/identity` entry point alone loads optional `@azure/identity` 4.14.
Alternatively pass the SDK's shared-key credential, another TokenCredential, or a
`Redacted` container URL containing an existing service/account SAS. Never pass a
plain string containing SAS query parameters. Native clients remain an escape
hatch; configuration and checkpoints exclude credentials and SAS values.

Owned clients use one SDK attempt. `makeClient(client, config, { sdkRetries:
"disabled" })` and `layerClient` borrow container clients and require a stable
explicit `storeId` and matching URL. The SDK hides its pipeline, so that third
argument is the caller's explicit attestation that maxTries is 1; it is not runtime
introspection. Build a container client from a caller-owned service client when
needed. `requestTimeoutMillis` aborts each request; download body auto-recovery is
disabled. SDK credential refresh is separate from protocol attempt accounting.

| Surface                                      | Behavior                                                                                                 |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `put`, `head`, `get`, `delete`, `listPage`   | Portable block-blob operations with scoped read bodies and bound cursors                                 |
| Portable conditions                          | Create-only, If-Match writes/deletes, conditional reads; suffix reads pin the preliminary size with ETag |
| `blocks.stage/list/commit`                   | Native named blocks, committed/uncommitted inventory and ordered publication                             |
| `acquireLease`, `renewLease`, `releaseLease` | Explicit native blob leases with Redacted lease IDs                                                      |
| `beginCopy`, `pollCopy`, `cancelCopy`        | Started copy ID, Pending/Completed/Failed status, native cancellation                                    |
| `AzureStorage.publicUrls(baseUrl?)`          | Explicit public-delivery service; does not make a container public                                       |

ETags, version IDs and snapshot IDs remain distinct. Versioned reads use their
native selectors. Downloads obtain body and metadata from one SDK response; SDK
response getters are read directly. Range content length and complete object size
remain separate. Full-object MD5 metadata is an observation, not a range checksum.
A current-object suffix read performs a preliminary HEAD and conditions GET on
that exact ETag, so replacements cannot splice bytes. Native If-None-Match `*`
is implemented by an existence/metadata lookup because Azure rejects that GET
condition. No body is fabricated for NotModified.

Delete never supplies an implicit deleteSnapshots option. Select a snapshot or
version explicitly; a blob with snapshots may reject an ordinary delete.
Non-block blobs fail explicitly. Blob names are limited to 1024 characters; dot
segments are rejected before I/O because the SDK otherwise normalizes them into a
different blob name. Provider admission also runs before managed source reads. Native HTTP header values and metadata values must be ASCII; encoded header
parameters can carry Unicode filenames. Metadata keys follow Azure identifier rules;
`effect-storage-operation` is mapped to reserved `effect_storage_operation` on
the wire and restored on reads. Application metadata cannot use the reserved alias.

`leaseId` captures a caller-owned lease for reads, uploads, blocks and publication.
Callers renew leases for long operations. A blob lease does not coordinate the
checkpoint store or make its exclusive ownership promise distributed.
`encryption` selects either `{ scope }` or `{ key: Redacted<string>, keySHA256 }`.
Customer keys must be canonical base64 AES-256 keys with the matching SHA-256;
encryption requires HTTPS. Secrets never enter checkpoints. The optional
`checksumAlgorithm: "StorageCrc64"` selects the SDK's native streaming framing
and validation on supporting services; it has no hosted evidence yet. The default
`"None"` avoids implicit new-service framing in the qualified local profile.
Native managed uploads independently verify the published object using SHA-256.

Primitive puts are capped at 256 MiB. Known-length streams use backpressure;
unknown lengths stage within `maxUnknownLengthBytes` (default 16 MiB) and `maxBytes`.
Source errors and EOF are joined before returning a successful receipt.

Managed uploads auto-select named blocks above the single-buffer threshold, or
accept `strategy: "blocks"`. Session IDs produce deterministic equal-length base64
block IDs. Recovery lists native inventory and reuses a block only with its saved
size and matching source checksum; uncertain stages can be safely repeated with
the same bytes. Commit orders only this session's IDs and retains headers,
conditions, encryption and lease settings. Foreign block writers with incompatible
ID widths may cause a native rejection rather than being silently adopted.

Block cancellation is abandonment. It never deletes or overwrites the existing
committed blob and cannot remove one uncommitted block. Azure's native expiry owns
that storage cleanup. Durable source fingerprints, protected checkpoints, lifetime
attempts and exact destination verification use the same session API as S3/GCS.
A lost commit response requires operation-marker/size reconciliation, followed by
pinned SHA-256 verification. A managed native receipt also requires full readback;
account for that read and its workspace budget.

`AzureSigning.layer(authority)` from `/azure/signing` accepts explicit shared-key
(service SAS) or Redacted user-delegation-key authority. SAS expiry is capped by
that authority; delegation SAS start is bounded by the key's start time. Required
PUT headers include `x-ms-blob-type: BlockBlob`. Read grants preserve version or
snapshot identity. Encrypted/leased profiles and versioned mutation grants reject
unsupported combinations. Account SAS issuance stays with the native SDK because
its account-wide scope is not an object-scoped StorageSigning grant. Existing
account SAS credentials are still usable by the data adapter.

Explicit encryption profiles reject async copy before initiation; managed copy
retains the configured encryption on its write path. `beginCopy` returns a Started ID, including when the SDK has no completed result.
`pollCopy` checks that ID and returns metadata only after native success. Pending,
failed and aborted states do not masquerade as copied bytes. Managed copy uses the
shared streamed-copy path with pinned source/destination verification.

Run `pnpm test:native:local`; see [phase 7 evidence](evidence/phase-7.md) for hosted
setup and evidence limits. Example: `packages/storage/examples/azure.mjs`.

See [runnable native examples and environment settings](native-examples.md).
