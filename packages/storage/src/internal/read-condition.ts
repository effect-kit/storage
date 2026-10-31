import type { ReadCondition } from "../core/requests.js";

/** Filter an acquired representation without a second, potentially racing metadata request. */
export function readCondition(condition: ReadCondition | undefined, etag: string | undefined) {
  if (!condition) return "modified";
  if (condition.etag === "*") return condition._tag === "IfMatch" ? "modified" : "not-modified";
  if (condition._tag === "IfMatch")
    return !condition.etag.startsWith("W/") && !etag?.startsWith("W/") && condition.etag === etag
      ? "modified"
      : "failed";
  return etag !== undefined && condition.etag.replace(/^W\//u, "") === etag.replace(/^W\//u, "")
    ? "not-modified"
    : "modified";
}
