import defineConfig from "klarity/vitest/node";

export default defineConfig({
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/azure/*.acceptance.ts"],
    fileParallelism: false,
    testTimeout: 60000,
    hookTimeout: 30000,
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/azure-local-tests.json" },
  },
});
