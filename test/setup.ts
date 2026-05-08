import { beforeAll, beforeEach, afterAll } from "vitest";

const doRedisSetup = () => {
  // REDIS_HOST and REDIS_PORT are set by the root-level globalSetup
  // (test/global-setup/redis.ts) and inherited by worker forks via process.env.
  // No inject() needed since the values arrive through the environment.
  const host = process.env.REDIS_HOST!;
  const port = Number(process.env.REDIS_PORT);

  // Generate the fork-scoped id prefix here (this setup file is loaded first
  // per fork) and forward via process.env. Vitest's loader may see separate
  // ESM (setupFiles) and CJS (test/bottleneck.js via require) module graphs,
  // so generating the prefix in one place and reading via env is the only way
  // to guarantee both sides agree on a single value per fork.
  const prefix = `t-${process.pid}-${Math.random().toString(36).slice(2, 8)}-`;
  process.env.BOTTLENECK_TEST_PREFIX = prefix;

  type FlushClient = {
    sendCommand(args: (string | number)[]): Promise<unknown>;
    quit?(): Promise<unknown>;
  };
  let flushClient: FlushClient | undefined;

  beforeAll(async () => {
    const { createClient } = await import("redis");
    flushClient = (await createClient({
      socket: { host, port },
    }).connect()) as unknown as FlushClient;
  });

  beforeEach(async () => {
    if (!flushClient) return;
    // Bottleneck stores every key as `b_<id>_<bucket>`; scan only this fork's
    // namespace and UNLINK in batches to keep cleanup non-blocking on Redis.
    const match = `b_${prefix}*`;
    let cursor: string | number = "0";
    do {
      const reply = (await flushClient.sendCommand([
        "SCAN",
        cursor,
        "MATCH",
        match,
        "COUNT",
        "1000",
      ])) as [string, string[]];
      cursor = reply[0];
      const batch = reply[1];
      if (batch.length > 0) {
        await flushClient.sendCommand(["UNLINK", ...batch]);
      }
    } while (String(cursor) !== "0");
  });

  afterAll(async () => {
    await flushClient?.quit?.();
  });
};

if (process.env.DATASTORE === "redis" || process.env.DATASTORE === "ioredis") {
  doRedisSetup();
}
