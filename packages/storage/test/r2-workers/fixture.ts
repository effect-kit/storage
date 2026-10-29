import type { R2BucketTransport, R2ObjectLike } from "../../src/r2-workers/binding.js";

export const object = (overrides: Partial<R2ObjectLike> = {}): R2ObjectLike => ({
  key: "object",
  size: 3,
  etag: "opaque",
  httpEtag: '"opaque"',
  uploaded: new Date("2026-01-01T00:00:00Z"),
  checksums: {},
  ...overrides,
});
export const bucket = (overrides: Partial<R2BucketTransport> = {}): R2BucketTransport => ({
  head: async () => null,
  get: async () => null,
  put: async (key) => object({ key }),
  delete: async () => undefined,
  list: async () => ({ objects: [], truncated: false, delimitedPrefixes: [] }),
  createMultipartUpload: async () => {
    throw new Error("unexpected native initiation");
  },
  resumeMultipartUpload: () => {
    throw new Error("unexpected native resume");
  },
  ...overrides,
});
