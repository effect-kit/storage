import { fileURLToPath } from "node:url";

import defineConfig from "klarity/vitest/node";

export default defineConfig({
  root: fileURLToPath(new URL("../", import.meta.url)),
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: { include: ["test/unit/**/*.test.ts", "test/integration/**/*.test.ts"] },
});
