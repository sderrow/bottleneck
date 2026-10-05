import IORedis from "ioredis";
import { ConnectionTimeoutError, SocketClosedUnexpectedlyError } from "redis";
import RedisClient from "redis";
import type BottleneckBase from "../../src/Bottleneck";
import type { IORedisClient, NodeRedisClient } from "../../src/cluster/redis-types";
import type { BottleneckOptions } from "../../src/types";
import Bottleneck from "../bottleneck";
import buildClientOptions from "../redis-client-options";

/** Log an error event unless it's a known transient connection error. */
function logUnexpectedError(source: string, err: unknown): void {
  const e = err as { code?: string; syscall?: string };
  const isIoredisConnectTimeout = e?.code === "ETIMEDOUT" && e?.syscall === "connect";
  const isTransientNodeRedisError =
    err instanceof ConnectionTimeoutError || err instanceof SocketClosedUnexpectedlyError;
  if (isIoredisConnectTimeout || isTransientNodeRedisError) return;

  console.log(`(${source}) ERROR EVENT`, err);
}

function setRedisClientOptions(options: Record<string, unknown>) {
  if (options.clientOptions == null) {
    options.clientOptions = buildClientOptions(String(options.datastore));
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
  options: Record<string, unknown> = {},
  meta: { expectErrors?: boolean } = {},
): BottleneckBase {
  const assigned = Object.assign({}, options) as Record<string, unknown>;
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

  const limiter = new Bottleneck(options as BottleneckOptions);

  if (!meta.expectErrors) {
    limiter.on("error", (err) => logUnexpectedError("makeLimiter", err));
  }

  // makeLimiter is synchronous; suppress the unhandled-rejection from ready()
  // for tests that never await it (connection-failure tests assert via the
  // "error" event instead).
  limiter.ready().catch(() => {});

  return limiter;
}

export default makeLimiter;
export { makeLimiter, buildClientOptions };

/**
 * A client for the test Redis of the current datastore (connection started,
 * error listener attached), which the caller owns and must close with
 * closeTestClient. Unexpected errors are logged unless `expectErrors`.
 */
export function makeTestClient(
  clientOptions?: Record<string, unknown>,
  meta: { expectErrors?: boolean } = {},
): NodeRedisClient | IORedisClient {
  const onError = (err: unknown) => {
    if (!meta.expectErrors) logUnexpectedError("makeTestClient", err);
  };
  if (process.env.DATASTORE === "ioredis") {
    const client = new IORedis(clientOptions ?? buildClientOptions("ioredis"));
    client.on("error", onError);
    return client;
  }
  const client = RedisClient.createClient(clientOptions ?? buildClientOptions("redis"));
  client.on("error", onError);
  // isOpen turns true synchronously, which is all RedisConnection needs.
  client.connect().catch(onError);
  return client;
}

/** Close a client from makeTestClient. */
export async function closeTestClient(client: NodeRedisClient | IORedisClient): Promise<void> {
  if ("status" in client) {
    client.disconnect();
  } else if (client.isOpen) {
    await (typeof client.destroy === "function" ? client.destroy() : client.disconnect());
  }
}
