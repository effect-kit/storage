# Portable storage

The core API and memory reference are implemented. S3, managed transfers and
durable sessions use separate entry points. Core imports depend only on Effect.

## Service and requests

`Storage` represents one configured namespace. Its five primitives are:

| Method     | Result                      | Behavior                                                      |
| ---------- | --------------------------- | ------------------------------------------------------------- |
| `put`      | ObjectMetadata              | Bytes or an Effect byte stream; overwrite by default          |
| `get`      | Object or NotModified       | Scoped, single-use body with matching metadata                |
| `head`     | ObjectMetadata              | Confirmed missing is NotFound                                 |
| `delete`   | void                        | Missing target is idempotent success unless a condition fails |
| `listPage` | entries and optional cursor | One bounded page; common prefixes are explicit entries        |

Put takes `key`, `body`, optional content headers/customMetadata, contentLength,
maxBytes and condition. Read/head/delete select exactly one of `{ key }` or
`{ ref }`. Foreign store references fail before reaching the adapter.

ObjectKey preserves slashes, percent sequences and Unicode normalization. Empty
keys, unpaired surrogates and NUL are rejected; a cloud key remains distinct from
a filesystem path. Headers and validators reject CR/LF/NUL. Custom metadata
names use ASCII letters/digits/dot/underscore/hyphen. Providers can impose narrower
limits and encoding rules.

ObjectSize is a nonnegative safe-integer JSON number. Native versions/generations
stay strings with their native tagged kind. ObjectMetadata's size describes the
complete object when known; get.contentLength describes the returned body.
Optional lastModified is DateTime.Utc in process, ISO text through its Schema codec.
ETags are opaque validators, not content checksums. Checksum values carry algorithm,
encoding and full-object/composite meaning separately.

## Conditions and capabilities

Put supports CreateOnly and IfMatch where advertised. Delete supports IfMatch.
Reads support IfMatch and IfNoneMatch. Validators are one opaque value or `*`;
IfMatch wildcard requires an existing target. CreateOnly is atomic at publication.
The constructor never emulates it with head-then-write.

StorageCapabilities declares conditions, range forms, version-reference scope,
page limits, delimiter behavior and cursor persistence. Unsupported requests fail
before consuming a source when the capability is known to be absent.
Capabilities returned by Storage.make are frozen snapshots.

StorageSigning, StoragePublicUrls and StorageCopy are independent services.
Applications request them explicitly; core Storage does not imply signing authority.
SignedRequest keeps bearer URLs/headers in Redacted values and represents constraint
enforcement separately. Public URL resolution does not change visibility.

Public multipart, offset/block, version/tag/retention/admin interfaces establish
extension boundaries. They are contracts, not implementations or support badges.
Native governance policy/state stays provider-specific.

## Streams and scopes

Upload bodies are `Uint8Array | Stream<Uint8Array, E, R>`. Put retains E and R.
Caller failures, defects and interruption are not converted wholesale to StorageError.
Do not mutate a source buffer while its upload is running.

```ts
const read = Effect.scoped(
  Effect.gen(function* () {
    const storage = yield* Storage;
    const response = yield* storage.get({
      key: "report.bin",
      range: { _tag: "From", start: 1024 },
    });

    if (response._tag === "NotModified") return "unchanged" as const;
    return yield* Stream.runFold(
      response.body,
      () => 0,
      (bytes, chunk) => bytes + chunk.length,
    );
  }),
);
```

Get and body consumption both require Scope. Storage.make guards each body against
consumption after its original scope closes (ScopeClosed) and a second materialization
(BodyConsumed). The adapter still owns actual response cancellation/descriptors;
the guard does not substitute for native finalizers.

Bounded, From and Suffix ranges use inclusive bounds. A range over an empty object
or starting beyond the object is unsatisfiable. A valid bounded range may clamp its
end to the complete size. NotModified has no body.

## Helpers

- `Storage.exists` catches only confirmed NotFound; denied/network failures propagate.
- `Storage.getBytes` requires maxBytes, checks declared and actual bytes and validates
  a known returned content length. It closes its scope on success or failure.
- `Storage.getText` has the same byte limit and rejects invalid UTF-8.
- Collection helpers surface NotModified as an explicit StorageError reason; use
  get when handling the low-level conditional result union.
- `Storage.list` emits Object/CommonPrefix entries lazily, follows empty continuation
  pages, and stops requesting pages when the consumer stops. A repeated continuation
  fails ProviderFailure rather than silently truncating.
- `Storage.deleteMany` uses bounded workers (default four) and preserves ordered
  per-request Result outcomes, including unknown mutations. Defects/interruption
  remain distinct; later managed bulk transfers add durable/lazy workflow behavior.

Collection byte limits bound retained payload, not all runtime overhead. Concatenation
can temporarily retain another payload-sized allocation; SDK/source buffers and
caller-retained objects are additional memory. See [managed transfers](transfers.md) for execution admission budgets.

## Failures and outcomes

StorageError has operation/provider, a tagged safe reason, mutationOutcome and
optional safe request correlation. It contains no raw SDK response, source bytes,
bearer URL or arbitrary cause. SDK adapters must sanitize their boundary mapping.

Use `Effect.catchReason("StorageError", "NotFound", ...)` or inspect the reason.
Mutation outcomes distinguish not-started, rejected, succeeded, unknown and partial;
reads use not-applicable. An unknown write may already exist. Cancellation does not
undo a committed mutation. Core does not automatically replay uploads or add retries.

## Adapter authors

Implement StorageService and use `Storage.make` to install request/capability/scope
guards, then supply it with an ordinary Layer. `Storage.of` remains the low-level
Effect service constructor for already guarded implementations; it adds no validation.
Keep identity stable and uniquely scoped. Native adapters capture SDK dependencies
at construction, preserve response identity and own resource finalizers.

Run the [public conformance suite](testing.md) with fresh caller-owned fixtures.
See the independently packed [custom adapter example](../packages/storage/test/consumers/custom-adapter.ts)
for core construction and an optional public-URL Layer. It is a small testing example,
not a production provider implementation.
