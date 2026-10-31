import { randomUUID } from "node:crypto";
import { Readable } from "node:stream";

import type { ObjectHeaders } from "../../core/object.js";
import { nativeHeaders } from "./mapping.js";

/** One JSON multipart exchange keeps primitive PUT request accounting exact. */
export function multipartBody(
  key: string,
  headers: ObjectHeaders,
  body: Uint8Array | Readable,
  length: number,
  crc32c?: string,
) {
  const boundary = randomUUID();
  const prefix = Buffer.from(
    `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${JSON.stringify({ name: key, ...nativeHeaders(headers), ...(crc32c ? { crc32c } : {}) })}\r\n--${boundary}\r\nContent-Type: ${headers.contentType ?? "application/octet-stream"}\r\n\r\n`,
  );
  const suffix = Buffer.from(`\r\n--${boundary}--\r\n`);
  return {
    length: prefix.length + length + suffix.length,
    contentType: `multipart/related; boundary=${boundary}`,
    body: Readable.from(
      (async function* () {
        yield prefix;
        if (body instanceof Readable) {
          for await (const chunk of body) yield chunk;
        } else yield body;
        yield suffix;
      })(),
    ),
  };
}
