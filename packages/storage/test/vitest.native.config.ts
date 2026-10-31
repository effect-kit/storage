import { fileURLToPath } from "node:url";

import defineConfig from "klarity/vitest/node";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/e2e/providers/azure.acceptance.ts"],
    fileParallelism: false,
    testTimeout: 60000,
    hookTimeout: 30000,
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/azure-local-tests.json" },
  },
});
