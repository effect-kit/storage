import defineConfig from "klarity/vitest/node";

export default defineConfig({
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/provider/native.hosted.ts"],
    fileParallelism: false,
    testTimeout: 120000,
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/native-hosted.json" },
  },
});
