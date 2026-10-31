import { gunzipSync } from "node:zlib";

const maximum = 128 * 1024 * 1024;
const text = (bytes: Uint8Array) =>
  new TextDecoder("utf-8", { fatal: true }).decode(bytes).split("\0")[0] ?? "";
const octal = (bytes: Uint8Array) => {
  const value = text(bytes).trim();
  if (!/^[0-7]+$/u.test(value)) throw new Error("Invalid archive numeric header");
  const result = Number.parseInt(value, 8);
  if (!Number.isSafeInteger(result)) throw new Error("Archive size overflow");
  return result;
};

/** Inspect bounded bytes without extracting, following links or invoking archive-controlled programs. */
export function inspectArchive(compressed: Uint8Array) {
  if (compressed.byteLength > 32 * 1024 * 1024) throw new Error("Packed archive exceeds limit");
  const tar = gunzipSync(compressed, { maxOutputLength: maximum });
  const files = new Map<string, Uint8Array>();
  let offset = 0;
  while (offset + 512 <= tar.length) {
    const header = tar.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) {
      if (tar.length - offset < 1024 || tar.subarray(offset).some((byte) => byte !== 0))
        throw new Error("Invalid archive end marker");
      return files;
    }
    const checksum = header.reduce(
      (total, byte, index) => total + (index >= 148 && index < 156 ? 32 : byte),
      0,
    );
    if (checksum !== octal(header.subarray(148, 156)))
      throw new Error("Archive header checksum mismatch");
    const prefix = text(header.subarray(345, 500));
    const name = `${prefix ? `${prefix}/` : ""}${text(header.subarray(0, 100))}`;
    if (
      !name.startsWith("package/") ||
      name.includes("\\") ||
      name
        .replace(/\/$/u, "")
        .split("/")
        .some((part) => !part || part.startsWith("."))
    )
      throw new Error("Unsafe archive path");
    const size = octal(header.subarray(124, 136));
    const kind = header[156];
    if (kind !== 0 && kind !== 48 && kind !== 53)
      throw new Error("Archive links or extended headers are forbidden");
    offset += 512;
    if (offset + size > tar.length || files.size > 10000)
      throw new Error("Truncated/oversized archive");
    if (kind === 53) {
      if (size !== 0) throw new Error("Directory contains payload");
    } else {
      if (files.has(name)) throw new Error("Duplicate archive entry");
      files.set(name, tar.subarray(offset, offset + size));
    }
    offset += Math.ceil(size / 512) * 512;
  }
  throw new Error("Archive has no complete end marker");
}
