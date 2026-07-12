// Root-level Vitest globalSetup: runs ONCE before any worker fork starts.
//
// A single Redis container is shared by both the ioredis and node-redis
// projects. Previously each project started its own container, which doubled
// Docker/VM overhead and caused the intermittent connection-timeout flakes
// (ConnectionTimeoutError, unexpected 5-second test durations, etc.).
//
// Key isolation between concurrent forks is handled by the per-fork id-prefix
// in test/bottleneck.js plus prefix-scoped SCAN+UNLINK in test/setup.ts, so
// sharing one instance is safe.
//
// Host/port are forwarded via process.env; worker forks inherit them
// automatically because they are spawned after this setup function runs.

import type { StartedRedisContainer } from "@testcontainers/redis";
// Untyped CJS module shared with src and the test helpers (see src/sleep.js).
import sleep from "../../src/sleep.js";

let stop: (() => Promise<unknown>) | undefined;

const START_ATTEMPTS = 3;
const RETRY_DELAY_MS = 2_000;

export async function setup(): Promise<void> {
  const { RedisContainer } = await import("@testcontainers/redis");

  // testcontainers hardcodes a 10s port-bind-inspection timeout that
  // withStartupTimeout cannot override (inspect-container-util-ports-exposed.js),
  // so under Docker Desktop churn .start() can time out even with margin to spare.
  // Retry with a fresh builder each attempt; containers leaked by a failed
  // attempt are reaped by ryuk at session end, and vitest applies no timeout
  // to root globalSetup, so the worst-case ~40s here is safe.
  let container: StartedRedisContainer | undefined;
  for (let attempt = 1; attempt <= START_ATTEMPTS; attempt++) {
    try {
      container = await new RedisContainer("redis:7-alpine")
        .withStartupTimeout(30_000)
        .withCommand(["redis-server", "--save", "", "--appendonly", "no"])
        .start();
      break;
    } catch (err) {
      if (attempt === START_ATTEMPTS) throw err;
      console.warn(
        `[global-setup] Redis container start failed (attempt ${attempt}/${START_ATTEMPTS}): ${err}; retrying in ${RETRY_DELAY_MS}ms`,
      );
      await sleep(RETRY_DELAY_MS);
    }
  }

  const started = container!;

  process.env.REDIS_HOST = started.getHost();
  process.env.REDIS_PORT = String(started.getPort());

  stop = () => started.stop();
}

export async function teardown(): Promise<void> {
  if (stop) await stop();
}
