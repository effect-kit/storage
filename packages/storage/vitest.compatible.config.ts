import defineConfig from "klarity/vitest/node";

export default defineConfig({
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/provider/compatible.acceptance.ts"],
    testTimeout: 90_000,
    fileParallelism: false,
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/compatible-acceptance.json" },
  },
});
