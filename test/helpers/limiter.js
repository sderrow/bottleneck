import IORedis from "ioredis";
import { ConnectionTimeoutError, SocketClosedUnexpectedlyError } from "redis";
import RedisClient from "redis";
import Bottleneck from "../bottleneck.mjs";
import buildClientOptions from "../redis-client-options";

function setRedisClientOptions(options) {
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
function makeLimiter(options, meta) {
  options = Object.assign({}, options);
  meta = meta || {};

  if (options.datastore == null) {
    if (process.env.DATASTORE === "redis") {
      options.datastore = "redis";
      options.Redis ??= RedisClient;
    } else if (process.env.DATASTORE === "ioredis") {
      options.datastore = "ioredis";
      options.Redis ??= IORedis;
    } else {
      options.datastore = "local";
    }
  }

  if (options.datastore === "redis" || options.datastore === "ioredis") {
    setRedisClientOptions(options);
  }

  const limiter = new Bottleneck(options);

  if (!meta.expectErrors) {
    limiter.on("error", (err) => {
      const isIoredisConnectTimeout = err?.code === "ETIMEDOUT" && err?.syscall === "connect";
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
