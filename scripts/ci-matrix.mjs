#!/usr/bin/env node
// Single source of truth for the CI test matrices, consumed by:
//   - .github/workflows/ci.yaml — the "matrix-data" prepare job runs
//     `node scripts/ci-matrix.mjs --emit >> "$GITHUB_OUTPUT"` and the matrix
//     jobs feed those outputs into strategy.matrix via fromJSON.
//   - scripts/test-ci.mjs — the local CI runner imports these exports to
//     recreate the exact same legs locally (`pnpm run test:ci`).
//
// Edit here to change CI coverage (a new server image, a new client pin):
// every matrix leg updates at once, so the workflow and the local runner
// cannot drift apart.

import { pathToFileURL } from "node:url";

// Every redis-compatible server image CI exercises. The local runner and
// `pnpm test` default to the newest entry (see test/global-setup/redis.ts),
// which is why the newest image needs no dedicated pin elsewhere.
export const CLUSTER_IMAGES = [
  "redis:6-alpine",
  "redis:7-alpine",
  "valkey/valkey:8-alpine",
  "valkey/valkey:9-alpine",
];

// Old clients are paired with the oldest supported server (the realistic
// combination: apps pin old clients against managed Redis that lags); new
// clients × every server image are already covered by the cluster legs via
// the committed lockfile, so only the client version varies here.
export const CLIENT_MATRIX_IMAGE = "redis:6-alpine";

// Varies the CLIENT library versions. Each vitest project uses exactly one
// client, so each leg overrides a single devDependency and runs only the
// matching project — no cross-product legs.
export const CLIENT_PINS = [
  { client: "ioredis", version: "5", project: "ioredis" },
  { client: "redis", version: "4", project: "node-redis" },
  { client: "redis", version: "5", project: "node-redis" },
];

// CLI: emit GitHub Actions output assignments for ci.yaml's matrix-data job.
// Only runs on direct execution, never when test-ci.mjs imports this module.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (process.argv[2] !== "--emit") {
    console.error("usage: node scripts/ci-matrix.mjs --emit");
    process.exit(2);
  }
  console.log(`cluster-images=${JSON.stringify(CLUSTER_IMAGES)}`);
  console.log(
    `client-matrix=${JSON.stringify({
      include: CLIENT_PINS.map((pin) => Object.assign({}, pin, { image: CLIENT_MATRIX_IMAGE })),
    })}`,
  );
}
