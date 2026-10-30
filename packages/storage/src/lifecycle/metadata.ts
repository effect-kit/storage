import type { MetadataMerge } from "../lifecycle.js";
import type { ObjectHeaders } from "../object.js";

export function mergeHeaders(
  current: ObjectHeaders,
  patch: (typeof MetadataMerge.Type)["patch"],
): ObjectHeaders {
  const custom = new Map(Object.entries(current.customMetadata ?? {}));
  for (const [key, value] of Object.entries(patch.customMetadata ?? {})) {
    if (value === null) custom.delete(key);
    else custom.set(key, value);
  }
  return {
    contentType: patch.contentType ?? current.contentType,
    contentEncoding: patch.contentEncoding ?? current.contentEncoding,
    contentLanguage: patch.contentLanguage ?? current.contentLanguage,
    contentDisposition: patch.contentDisposition ?? current.contentDisposition,
    cacheControl: patch.cacheControl ?? current.cacheControl,
    customMetadata: Object.fromEntries(custom),
  };
}

/** Replace custom metadata; retain system headers omitted by the caller. */
export function replaceHeaders(current: ObjectHeaders, replacement: ObjectHeaders): ObjectHeaders {
  return {
    contentType: replacement.contentType ?? current.contentType,
    contentEncoding: replacement.contentEncoding ?? current.contentEncoding,
    contentLanguage: replacement.contentLanguage ?? current.contentLanguage,
    contentDisposition: replacement.contentDisposition ?? current.contentDisposition,
    cacheControl: replacement.cacheControl ?? current.cacheControl,
    customMetadata: replacement.customMetadata ?? {},
  };
}
