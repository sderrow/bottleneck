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

let stop: (() => Promise<unknown>) | undefined;

export async function setup(): Promise<void> {
  const { RedisContainer } = await import("@testcontainers/redis");

  const container = await new RedisContainer("redis:7-alpine")
    .withStartupTimeout(30_000)
    .withCommand(["redis-server", "--save", "", "--appendonly", "no"])
    .start();

  process.env.REDIS_HOST = container.getHost();
  process.env.REDIS_PORT = String(container.getPort());

  stop = () => container.stop();
}

export async function teardown(): Promise<void> {
  if (stop) await stop();
}
