import defineConfig from "klarity/tsdown/library";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/testing.ts",
    "src/s3.ts",
    "src/s3/signing.ts",
    "src/s3/post.ts",
    "src/transfers.ts",
    "src/sessions.ts",
    "src/node.ts",
    "src/bun.ts",
    "src/deno.ts",
    "src/browser.ts",
    "src/filesystem.ts",
    "src/r2-workers.ts",
    "src/gcs.ts",
    "src/gcs/signing.ts",
    "src/gcs/post.ts",
    "src/azure.ts",
    "src/azure/signing.ts",
    "src/azure/identity.ts",
  ],
  unbundle: true,
  deps: { neverBundle: [/^node:/u] },
  exports: false,
  dts: { sourcemap: false },
});
