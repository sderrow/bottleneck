import IORedis from "ioredis";
import { ConnectionTimeoutError, SocketClosedUnexpectedlyError } from "redis";
import RedisClient from "redis";
import type { IORedisClient, NodeRedisClient } from "../../src/cluster/redis-types";
import buildClientOptions from "../redis-client-options";

/** Log an error event unless it's a known transient connection error. */
export function logUnexpectedError(source: string, err: unknown): void {
  const e = err as { code?: string; syscall?: string };
  const isIoredisConnectTimeout = e?.code === "ETIMEDOUT" && e?.syscall === "connect";
  const isTransientNodeRedisError =
    err instanceof ConnectionTimeoutError || err instanceof SocketClosedUnexpectedlyError;
  if (isIoredisConnectTimeout || isTransientNodeRedisError) return;

  console.log(`(${source}) ERROR EVENT`, err);
}

/**
 * A client for the test Redis of the current datastore (connection started,
 * error listener attached), which the caller owns and must close with
 * closeTestClient. Unexpected errors are logged unless `expectErrors`.
 */
export function makeTestClient(
  clientOptions?: Record<string, unknown>,
  meta: { expectErrors?: boolean; datastore?: string } = {},
): NodeRedisClient | IORedisClient {
  const datastore = meta.datastore ?? process.env.DATASTORE;
  const onError = (err: unknown) => {
    if (!meta.expectErrors) logUnexpectedError("makeTestClient", err);
  };
  if (datastore === "ioredis") {
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

/**
 * Close a client from makeTestClient. node-redis clients close gracefully:
 * destroy() rejects whatever node-redis itself still has queued, which
 * surfaced as unhandled "Disconnects client" rejections in Group teardown
 * even with none of Bottleneck's commands in flight.
 */
export async function closeTestClient(client: NodeRedisClient | IORedisClient): Promise<void> {
  if ("status" in client) {
    client.disconnect();
  } else if (client.isOpen) {
    await (typeof client.close === "function" ? client.close() : client.quit());
  }
}
