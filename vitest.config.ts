import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  // The same source-not-dist resolution the root tsconfig's `paths` set up for
  // tsc. `packages/backend-redis/src` imports `@dianemo/core` by name, so
  // without this every suite resolves it through the workspace symlink to a
  // `dist` that a fresh clone has not built yet and fails to collect.
  resolve: {
    alias: {
      "@dianemo/core": fileURLToPath(
        new URL("./packages/core/src/index.ts", import.meta.url)
      ),
      "@dianemo/backend-redis": fileURLToPath(
        new URL("./packages/backend-redis/src/index.ts", import.meta.url)
      ),
    },
  },
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    // Run test files one at a time. Several suites assert on timing and on
    // occupancy at a shared HTTP upstream, and four of them competing for one
    // CPU and one Redis made those assertions measure the machine rather than
    // the code — a suite that timed out would leave its client still draining
    // into the next suite's measurements.
    fileParallelism: false,
    typecheck: {
      // Type-level assertions guard the plugin namespace inference — if `use()`
      // ever returns `any`, the runtime tests would still pass while every
      // consumer silently loses type safety.
      include: ["test/**/*.test-d.ts"],
      tsconfig: "./tsconfig.json",
    },
    coverage: {
      include: ["packages/*/src/**/*.ts"],
    },
  },
});
