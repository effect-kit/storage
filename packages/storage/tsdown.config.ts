import defineConfig from "klarity/tsdown/library";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/testing.ts",
    "src/s3.ts",
    "src/s3/signing.ts",
    "src/s3/post.ts",
    "src/transfers.ts",
    "src/transfers/node.ts",
    "src/s3/transfers.ts",
  ],
  unbundle: true,
  deps: { neverBundle: [/^node:/u] },
  exports: false,
  dts: { sourcemap: false },
});
