import { Effect } from "effect";

import { StoragePublicUrls } from "../core/capabilities.js";
import { storageError } from "../core/errors.js";
import { ObjectRef } from "../core/object.js";
import type { StorageService } from "../core/storage.js";
import { decodeRequest } from "./request-validation.js";

/** Caller explicitly selects public delivery; resolving a URL never changes visibility. */
export const makePublicUrls = Effect.fnUntraced(function* (
  storage: StorageService,
  baseUrl: string,
) {
  const base = URL.parse(baseUrl);
  if (
    !base ||
    !["https:", "http:"].includes(base.protocol) ||
    base.username ||
    base.password ||
    base.search ||
    base.hash ||
    !base.pathname.endsWith("/")
  )
    return yield* storageError("publicUrls", storage.provider, {
      _tag: "InvalidConfiguration",
      field: "baseUrl",
    });
  return StoragePublicUrls.of({
    url: Effect.fnUntraced(function* (ref) {
      yield* decodeRequest(ObjectRef, ref, "url", storage.provider);
      if (ref.storeId !== storage.storeId)
        return yield* storageError("url", storage.provider, {
          _tag: "InvalidInput",
          field: "ref.storeId",
        });
      if (ref.version || ref.key.split("/").some((segment) => segment === "." || segment === ".."))
        return yield* storageError("url", storage.provider, {
          _tag: "Unsupported",
          capability: "version-or-dot-segment-public-url",
        });
      return base.href + ref.key.split("/").map(encodeURIComponent).join("/");
    }),
  });
});
