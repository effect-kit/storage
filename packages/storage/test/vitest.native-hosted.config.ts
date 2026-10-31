import { fileURLToPath } from "node:url";

import defineConfig from "klarity/vitest/node";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/e2e/providers/native.hosted.ts"],
    fileParallelism: false,
    testTimeout: 120000,
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/native-hosted.json" },
  },
});
