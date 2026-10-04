import type BottleneckBase from "../../src/Bottleneck";
import type RedisDatastore from "../../src/cluster/RedisDatastore";
import type LocalDatastore from "../../src/LocalDatastore";
import * as Scripts from "../../src/cluster/Scripts";

/** The limiter's datastore, narrowed to the Redis implementation. */
export function redisStore(limiter: BottleneckBase): RedisDatastore {
  const store = limiter._store;
  if (!("originalId" in store)) throw new Error("Expected a Redis-backed limiter");
  return store;
}

/** The limiter's datastore, narrowed to the local implementation. */
export function localStore(limiter: BottleneckBase): LocalDatastore {
  const store = limiter._store;
  if ("originalId" in store) throw new Error("Expected a local limiter");
  return store;
}

type LimiterKeys = [
  settings: string,
  jobWeights: string,
  jobExpirations: string,
  jobClients: string,
  clientRunning: string,
  clientNumQueued: string,
  clientLastRegistered: string,
  clientLastSeen: string,
];

/** The limiter's Redis keys, in `Scripts.allKeys` order. */
export function limiterKeys(limiter: BottleneckBase): LimiterKeys {
  const keys = Scripts.allKeys(redisStore(limiter).originalId);
  if (keys.length !== 8) throw new Error(`Expected 8 limiter keys, got ${keys.length}`);
  return keys as LimiterKeys;
}

/** Reply shapes (after normalizeReply) for the commands tests read. */
type Replies = {
  exists: number;
  hget: string | null;
  hgetall: Record<string, string>;
  hkeys: string[];
  hlen: number;
  hmget: (string | null)[];
  hvals: string[];
  ttl: number;
  zcard: number;
  zrange: string[];
  zscore: string | null;
};

/** Run a raw Redis command on the limiter's connection. */
export function runCommand<C extends string>(
  limiter: BottleneckBase,
  command: C,
  args: string[],
): Promise<C extends keyof Replies ? Replies[C] : unknown> {
  return redisStore(limiter).connection.__runCommand__([command, ...args]) as Promise<
    C extends keyof Replies ? Replies[C] : unknown
  >;
}
