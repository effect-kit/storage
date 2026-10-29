# 010 — Native R2 binding capabilities

Status: implemented; local workerd acceptance passes, hosted qualification pending.

Use the caller's native R2Bucket authority directly. No AWS/Cloudflare management SDK,
credential chain or client ownership is introduced. Structural binding results keep
Node and global Workers declarations out of public consumers. The narrow constructor
boundary interprets native writable bodies once because DOM and Workers reader overloads
differ; assignment from the official development types is checked.

Map metadata-only reads/null conditional writes explicitly and translate HTTP-form
validators to unquoted native conditions. Preserve same-response metadata and stored
bytes. Scoped readers own body cancellation. Known streams use FixedLengthStream;
unknown primitive streams stage under an explicit bound. Source errors remain typed.
Binding interruption cannot prove a remote write stopped; unknown outcomes and native
request-budget limitations remain documented.

Provide one matching TransferDriver with real numbered-part limits and no inventory.
The protocol contract adds optional completeConditions; R2 declares none. Planning
rejects conditional multipart before source reads or remote mutation. Existing adapters
omitting the field retain the previous full completion contract. Copy/session planning
uses the same condition gate. Unknown ordinary completions require exact marker/size
proof; key equality alone is insufficient.

Do not supply a durable multipart handle codec until the required recovery guarantees
exist. Reconstructing an upload handle is local, not remote existence evidence. Expose
Unsupported inventory rather than an empty list. Download sessions retain application
backing/target/ownership requirements. The local memory-backed fixture qualifies no
cross-invocation persistence. No signing, native copy, conditional delete or admin
capability is inferred from a bucket binding; public delivery is an explicit resolver.

Local acceptance uses exact archives, a server-free bundle, disabled outbound network
and an owned Miniflare/workerd bucket. The fixture is guarded as local-only and resets
only that bucket. Future hosted tests require independently authorized resources and
must not reuse the destructive local reset fixture against application data.
