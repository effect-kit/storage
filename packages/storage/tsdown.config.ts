import defineConfig from "klarity/tsdown/library";

export default defineConfig({
  entry: ["src/index.ts", "src/testing.ts"],
  unbundle: true,
  exports: false,
  dts: { sourcemap: false },
});
