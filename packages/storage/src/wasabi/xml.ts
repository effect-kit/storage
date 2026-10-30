import { Effect } from "effect";

import type { GetObjectCommandOutput } from "@aws-sdk/client-s3";
import { XMLParser, XMLValidator } from "fast-xml-parser";

import { storageError } from "../errors.js";

const parser = new XMLParser({
  ignoreAttributes: false,
  parseTagValue: false,
  processEntities: true,
});
export const readXml = Effect.fnUntraced(function* (
  body: GetObjectCommandOutput["Body"],
  operation: string,
) {
  if (!body || !(Symbol.asyncIterator in body))
    return yield* storageError(
      operation,
      "wasabi",
      { _tag: "ProviderFailure", code: "MissingXmlReceipt" },
      "unknown",
    );
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      if ("destroy" in body && typeof body.destroy === "function") body.destroy();
    }),
  );
  return yield* Effect.tryPromise({
    try: async () => {
      const chunks: Uint8Array[] = [];
      let size = 0;
      for await (const chunk of body) {
        size += chunk.byteLength;
        if (size > 4 * 1024 ** 2) throw new Error("Receipt exceeds limit");
        chunks.push(chunk);
      }
      const bytes = new Uint8Array(size);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      const xml = new TextDecoder().decode(bytes);
      if (/<!DOCTYPE|<!ENTITY/iu.test(xml) || XMLValidator.validate(xml) !== true)
        throw new Error("Invalid receipt");
      return { xml, decoded: parser.parse(xml) as unknown };
    },
    catch: () =>
      storageError(
        operation,
        "wasabi",
        { _tag: "ProviderFailure", code: "InvalidXmlReceipt" },
        "unknown",
      ),
  }).pipe(
    Effect.timeoutOrElse({
      duration: "30 seconds",
      orElse: () =>
        Effect.fail(
          storageError(
            operation,
            "wasabi",
            { _tag: "TransportFailure", code: "ReceiptDeadline" },
            "unknown",
          ),
        ),
    }),
  );
});
