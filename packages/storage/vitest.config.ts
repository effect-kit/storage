import defineConfig from "klarity/vitest/node";

export default defineConfig({
  envDir: false,
  resolve: { conditions: ["workspace-source"] },
  test: { include: ["test/**/*.test.ts"] },
});
