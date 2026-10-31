import { Context, Effect, Layer } from "effect";

import { storageError } from "../../core/errors.js";
import { decodeRequest } from "../../internal/request-validation.js";
import { MetadataReplace } from "../../lifecycle/index.js";
import { replaceHeaders } from "../../lifecycle/metadata.js";
import { encryption, lease } from "./config.js";
import { AzureStorage } from "./index.js";
import type { AzureStorageService } from "./index.js";
import { metadata, nativeHeaders, target } from "./mapping.js";
import { request } from "./transport.js";

const make = (storage: AzureStorageService) => ({
  /** Azure writes system headers separately from custom metadata. Each operation has its own CAS. */
  setHeaders: Effect.fnUntraced(function* (input: typeof MetadataReplace.Type) {
    const value = yield* decodeRequest(MetadataReplace, input, "setHeaders", "azure", true);
    if (value.ref.version || value.condition?._tag !== "IfMatch" || value.headers.customMetadata)
      return yield* storageError(
        "setHeaders",
        "azure",
        { _tag: "InvalidInput", field: "current-if-match-system-headers" },
        "not-started",
      );
    const address = yield* target(storage, { ref: value.ref }, "setHeaders");
    const conditions = { ifMatch: value.condition.etag, ...lease(storage.config) };
    const before = yield* request(storage, "setHeaders.head", false, (signal) =>
      address.client.getProperties({
        abortSignal: signal,
        conditions,
        ...encryption(storage.config),
      }),
    );
    const previous = metadata(storage, value.ref.key, before);
    const headers = replaceHeaders(previous, value.headers);
    const output = yield* request(storage, "setHeaders", true, (signal) =>
      address.client.setHTTPHeaders(nativeHeaders(headers), { abortSignal: signal, conditions }),
    );
    if (!output.etag)
      return yield* storageError(
        "setHeaders",
        "azure",
        { _tag: "ProviderFailure", code: "MissingHeadersReceipt" },
        "unknown",
      );
    return {
      ...metadata(storage, value.ref.key, { ...before, versionId: undefined, ...output }),
      ...value.headers,
    };
  }),
});
export class AzureProperties extends Context.Service<AzureProperties, ReturnType<typeof make>>()(
  "@effect-kit/storage/azure/AzureProperties",
) {
  static readonly make = make;
  static readonly layer = Layer.effect(AzureProperties, Effect.map(AzureStorage, make));
}
