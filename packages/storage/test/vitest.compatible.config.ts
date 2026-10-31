import { fileURLToPath } from "node:url";

import defineConfig from "klarity/vitest/node";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/e2e/providers/compatible.acceptance.ts"],
    testTimeout: 90_000,
    fileParallelism: false,
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/compatible-acceptance.json" },
  },
});
