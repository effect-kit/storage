import { fileURLToPath } from "node:url";

import defineConfig from "klarity/vitest/node";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: {
    include: ["test/e2e/providers/app.hosted.ts"],
    fileParallelism: false,
    testTimeout: 120000,
  },
});
