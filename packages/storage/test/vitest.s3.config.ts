import { fileURLToPath } from "node:url";

import defineConfig from "klarity/vitest/node";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: [
      "test/e2e/providers/s3.acceptance.ts",
      "test/e2e/providers/transfers.acceptance.ts",
      "test/e2e/providers/sessions.acceptance.ts",
    ],
    testTimeout: 90_000,
    hookTimeout: 30_000,
    fileParallelism: false,
    execArgv: ["--expose-gc"],
    reporters: ["default", "json"],
    outputFile: { json: "../../.artifacts/s3-acceptance.json" },
  },
});
