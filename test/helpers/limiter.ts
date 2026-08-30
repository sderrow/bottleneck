import IORedis from "ioredis";
import { ConnectionTimeoutError, SocketClosedUnexpectedlyError } from "redis";
import RedisClient from "redis";
import type BottleneckBase from "../../src/Bottleneck";
import type { ConstructorOptions } from "../../src/types";
import Bottleneck from "../bottleneck";
import buildClientOptions from "../redis-client-options";

function setRedisClientOptions(options: Record<string, any>) {
  if (options.clientOptions == null) {
    options.clientOptions = buildClientOptions(options.datastore);
  }
}

/**
 * Create a Bottleneck limiter pre-wired for the current test environment.
 *
 * @param {object} [options] - Bottleneck constructor options (datastore/clientOptions auto-filled from env)
 * @param {{ expectErrors?: boolean }} [meta] - Test-level flags kept separate from Bottleneck options
 * @returns {import("../../src/Bottleneck").default}
 */
function makeLimiter(
  options: Record<string, any> = {},
  meta: { expectErrors?: boolean } = {},
): BottleneckBase {
  const assigned = Object.assign({}, options) as Record<string, any>;
  options = assigned;

  if (options.datastore == null) {
    if (process.env.DATASTORE === "redis") {
      assigned.datastore = "redis";
      assigned.Redis ??= RedisClient;
    } else if (process.env.DATASTORE === "ioredis") {
      assigned.datastore = "ioredis";
      assigned.Redis ??= IORedis;
    } else {
      assigned.datastore = "local";
    }
  }

  if (options.datastore === "redis" || options.datastore === "ioredis") {
    setRedisClientOptions(options);
  }

  const limiter = new Bottleneck(options as ConstructorOptions);

  if (!meta.expectErrors) {
    limiter.on("error", (err) => {
      const e = err as { code?: string; syscall?: string };
      const isIoredisConnectTimeout = e?.code === "ETIMEDOUT" && e?.syscall === "connect";
      const isTransientNodeRedisError =
        err instanceof ConnectionTimeoutError || err instanceof SocketClosedUnexpectedlyError;
      if (isIoredisConnectTimeout || isTransientNodeRedisError) return;

      console.log("(makeLimiter) ERROR EVENT", err);
    });
  }

  // makeLimiter is synchronous; suppress the unhandled-rejection from ready()
  // for tests that never await it (connection-failure tests assert via the
  // "error" event instead).
  limiter.ready().catch(() => {});

  return limiter;
}

export default makeLimiter;
export { makeLimiter, buildClientOptions };
