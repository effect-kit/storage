# Filesystem object storage

`@effect-kit/storage/filesystem` implements Storage over Effect FileSystem/Path.
Supply a private application-owned root and stable storeId. The root is bound to
that identity in a versioned format file; reopening with another storeId fails.

```ts
import { Effect, Layer } from "effect";

import { Storage } from "@effect-kit/storage";
import { FilesystemStorage } from "@effect-kit/storage/filesystem";
import * as StorageNode from "@effect-kit/storage/node";

const FilesLive = FilesystemStorage.layer({
  root: config.objectDirectory,
  storeId: config.storageIdentity,
}).pipe(Layer.provide(StorageNode.layer));

const program = Effect.gen(function* () {
  const storage = yield* Storage;
  return yield* storage.put({
    key: "documents/hello.txt",
    body: new TextEncoder().encode("hello"),
    contentType: "text/plain",
    condition: { _tag: "CreateOnly" },
  });
});
await Effect.runPromise(program.pipe(Effect.provide(FilesLive)));
```

Use the same composition with StorageBun.layer for Bun. Node/Bun runtime modules
provide FileSystem, Path, bounded Digest, SecureRandom and incremental TransferHash.
The provider itself imports only Effect and shared library contracts. Basic setup
requires these services because writes calculate integrity and opaque generation
validators. This format requires a SHA-256 TransferHash; supply any different
managed hash override to the managed layer separately. FilesystemStorage.make returns a captured service; layer also supplies
Storage and a matching primitive-only TransferDriver.

## Layout and publication

The private root contains format.json, objects/ and staging/. Object filenames are
SHA-256 of the exact UTF-8 key, never paths derived from key segments. Traversal-looking,
absolute, Windows-looking, case-distinct and canonically distinct Unicode keys stay
separate logical keys. Keys are limited to 4096 UTF-8 bytes. No slash, percent or
Unicode normalization is performed. The envelope retains the original key and checks
its namespace/hash binding; a digest collision fails rather than aliasing on read.

Each object is one immutable envelope: a 64 KiB reserved header containing versioned
schema-validated metadata, followed by streamed body bytes. Put writes an exclusive
0600 staged file, checks actual length/limit, records a full-object SHA-256 checksum,
writes the final header, syncs and closes, then publishes the complete file. The
validator is a fresh opaque generation ID, not a claimed checksum or historic version.

Overwrite uses same-filesystem rename. CreateOnly hard-links the completed staged
file to the final name with no replacement. Independent processes can race and exactly
one publishes. The object directory is synced before success. Root creation syncs
new ancestor entries. Open readers retain their descriptor to the original envelope,
so metadata/body match even while another process replaces the current name.

## Capabilities and resource limits

Five primitives, ranges, read IfMatch/IfNoneMatch, create-only writes, prefix/delimiter
listing and query-bound persistent cursors are implemented. Match-current writes,
conditional deletion, historic version reads, signing and native copy/multipart are
not advertised. Process locks do not establish filesystem CAS.

Default maxObjectBytes is 1 GiB, configurable up to the safe envelope size limit.
Put supports unknown-length streams without collecting the body; per-request maxBytes
can lower the cap. Source E/R are preserved. Adapter writes and read chunks are 64 KiB;
caller source allocations remain caller-owned. Metadata supplied by callers is capped
at 32 KiB and the final encoded header must fit its reserved space.

Listing reads the directory array and scans envelopes, then sorts logical keys.
It is O(directory size), not a constant-memory database index. maxListEntries defaults
to 10,000, configurable up to one million; exceeding it fails explicitly. The bound
is checked after FileSystem.readDirectory allocates its array. Pages are capped at
1,000 and are not snapshots across concurrent mutations. Cursors bind root identity,
storeId, prefix, delimiter and limit. Keep this root dedicated to the library.

Managed Transfers works through the supplied driver. With no native multipart,
managed uploads use the engine's bounded single-request staging and must fit policy
admission; use primitive streaming puts for larger disk-only writes. Managed downloads
verify recorded checksums. Moves reject before copying without safe deletion
authority. Durable multipart upload sessions are unsupported; pinned
conditional download sessions can use the normal session target/store interfaces.

## Trust, cleanup and durability boundaries

The application owns root permissions and excludes hostile local writers/root
replacement. Existing permissions are not silently changed. Root/internal symlinks
and nonregular object files fail closed; no-follow opens protect final components.
This does not claim protection against hostile concurrent ancestor replacement,
external in-place edits or arbitrary storage tampering. Objects are not encrypted by
this adapter. Protection of session checkpoints remains separately configured.

Issued writes and publication join before scoped cleanup, including interruption.
Ordinary failures remove only their owned staging file. A killed process can leave
private staging debt. After excluding every writer across processes, explicitly call
`filesystem.reclaimStaging({ ownership: "exclusive" })`; it removes only recognized
regular staging files. There is no age-based sweeper that might delete an active writer.
Never run reclamation concurrently with any writer. Published objects are untouched.

A publication/directory-sync failure can leave an unknown mutation outcome. Process
crash tests do not prove physical power-cut, device-cache or network filesystem
behavior. The first qualified local profiles are Darwin arm64 Node/Bun; other OSes
require independent results. Unsupported atomic/no-follow/sync primitives fail rather
than falling back to weaker publication. See phase 6 evidence for executed lanes.
