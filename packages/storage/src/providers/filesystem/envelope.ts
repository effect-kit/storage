import { Effect, Option, Schema } from "effect";
import type { FileSystem } from "effect";

import { ObjectMetadata } from "../../core/object.js";
import { fileError, fileSize } from "./layout.js";
import type { FilesystemContext } from "./layout.js";

// One immutable file publishes bytes and metadata together, without sidecar races.
export const headerBytes = 65536;
const magic = new TextEncoder().encode("EFSTOR01");
const Envelope = Schema.fromJsonString(
  Schema.Struct({ version: Schema.Literal(1), metadata: ObjectMetadata }),
);

export const readExactly = Effect.fnUntraced(function* (
  file: FileSystem.File,
  length: number,
  position: number,
) {
  const bytes = new Uint8Array(length);
  let offset = 0;
  while (offset < length) {
    const count = yield* file.read(bytes.subarray(offset), { position: BigInt(position + offset) });
    if (count === 0) return yield* fileError("get", "TruncatedEnvelope");
    offset += count;
  }
  return bytes;
});

export const encodeEnvelope = Effect.fnUntraced(function* (metadata: ObjectMetadata) {
  const encoded = yield* Schema.encodeEffect(Envelope)({ version: 1, metadata }).pipe(
    Effect.mapError(() => fileError("put", "InvalidMetadata", "not-started")),
  );
  const bytes = new TextEncoder().encode(encoded);
  if (bytes.length > headerBytes - 12)
    return yield* fileError("put", "MetadataTooLarge", "not-started");
  const header = new Uint8Array(headerBytes);
  header.set(magic);
  new DataView(header.buffer).setUint32(8, bytes.length);
  header.set(bytes, 12);
  return header;
});

export const readEnvelope = Effect.fnUntraced(function* (
  context: FilesystemContext,
  file: FileSystem.File,
  key?: string,
) {
  const prefix = yield* readExactly(file, 12, 0);
  if (!magic.every((byte, index) => prefix[index] === byte))
    return yield* fileError("get", "InvalidEnvelope");
  const length = new DataView(prefix.buffer).getUint32(8);
  if (length < 1 || length > headerBytes - 12) return yield* fileError("get", "InvalidEnvelope");
  const encoded = yield* readExactly(file, length, 12);
  const text = yield* Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(encoded),
    catch: () => fileError("get", "InvalidEnvelope"),
  });
  const { metadata } = yield* Schema.decodeUnknownEffect(Envelope)(text).pipe(
    Effect.mapError(() => fileError("get", "InvalidEnvelope")),
  );
  if (
    metadata.ref.storeId !== context.storeId ||
    metadata.ref.version ||
    (key !== undefined && metadata.ref.key !== key) ||
    metadata.size === undefined ||
    !metadata.etag ||
    metadata.checksums?.length !== 1 ||
    metadata.checksums[0]?.algorithm !== "sha256" ||
    metadata.checksums[0]?.scope !== "full-object" ||
    fileSize(yield* file.stat) !== BigInt(headerBytes) + BigInt(metadata.size)
  )
    return yield* fileError("get", "InvalidEnvelope");
  return metadata;
});

export const readChunk = Effect.fnUntraced(function* (
  file: FileSystem.File,
  position: number,
  length: number,
) {
  const bytes = yield* file.readAlloc(length, { position: BigInt(position) });
  if (Option.isNone(bytes)) return yield* fileError("get", "TruncatedBody");
  return bytes.value;
});
