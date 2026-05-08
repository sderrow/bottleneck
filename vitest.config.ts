import { defineConfig } from "vitest/config";

const setupFile = "test/setup.ts";
const redisGlobalSetup = "test/global-setup/redis.ts";
const lightGlobalSetup = "test/global-setup/light.ts";
const libGlobalSetup = "test/global-setup/lib.ts";

const sourceInclude = ["test/**/*.test.js"];
const sourceExclude = ["test/smoke/**", "test/memory/**"];

export default defineConfig({
  test: {
    // Start ONE Redis container for the whole run (shared by ioredis + node-redis).
    // Previously each project started its own container, doubling Docker/VM load
    // and causing intermittent ConnectionTimeoutErrors / timing flakes.
    globalSetup: [redisGlobalSetup],
    projects: [
      {
        test: {
          name: "local",
          root: ".",
          include: sourceInclude,
          exclude: [...sourceExclude, "test/cluster*.test.js", "test/*redis.test.js"],
          setupFiles: [setupFile],
        },
      },
      {
        test: {
          name: "ioredis",
          root: ".",
          env: { DATASTORE: "ioredis" },
          include: sourceInclude,
          exclude: [...sourceExclude, "test/node_redis.test.js"],
          setupFiles: [setupFile],
          testTimeout: 15_000,
          hookTimeout: 30_000,
        },
      },
      {
        test: {
          name: "node-redis",
          root: ".",
          env: { DATASTORE: "redis" },
          include: sourceInclude,
          exclude: [...sourceExclude, "test/ioredis.test.js"],
          setupFiles: [setupFile],
          testTimeout: 15_000,
          hookTimeout: 30_000,
        },
      },
      {
        test: {
          name: "light-smoke",
          root: ".",
          env: { BOTTLENECK_ENTRY: "light" },
          include: ["test/smoke/light.test.js"],
          setupFiles: [setupFile],
          // Build dist/light.js before the smoke test reads it.
          globalSetup: [lightGlobalSetup],
        },
      },
      {
        // Smoke test for the full bundle (`dist/index.js`). Distinct from
        // light-smoke because this one runs against Redis to actually
        // exercise the `inline-lua` plugin's output via Scripts.js — that
        // wiring is invisible to the local-only path the light bundle
        // covers, and the regular cluster suite runs against `src/`.
        test: {
          name: "lib-smoke",
          root: ".",
          env: { BOTTLENECK_ENTRY: "lib", DATASTORE: "redis" },
          include: ["test/smoke/lib.test.js"],
          setupFiles: [setupFile],
          // Build dist/index.js before the smoke test reads it. The shared
          // Redis container is started by the root globalSetup above.
          globalSetup: [libGlobalSetup],
          testTimeout: 15_000,
          hookTimeout: 30_000,
        },
      },
      {
        test: {
          name: "memory",
          root: ".",
          include: ["test/memory/**/*.test.js"],
          execArgv: ["--expose-gc"],
          setupFiles: [setupFile],
        },
      },
    ],
  },
});
