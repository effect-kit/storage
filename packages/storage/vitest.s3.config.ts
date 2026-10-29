import defineConfig from "klarity/vitest/node";

export default defineConfig({
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/provider/s3.acceptance.ts", "test/provider/transfers.acceptance.ts"],
    testTimeout: 90_000,
    hookTimeout: 30_000,
    fileParallelism: false,
    execArgv: ["--expose-gc"],
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/s3-acceptance.json" },
  },
});
