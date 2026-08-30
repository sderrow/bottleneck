// Shared helper: returns the `clientOptions` object for the given datastore.
// Used by test/bottleneck.mjs (withRedis) and test/helpers/limiter.js (makeLimiter).
// Keeping it in one place ensures the socket-resilience settings below stay
// consistent across all Redis connections opened during tests.
//
// Both branches set short connect timeouts and aggressive reconnect strategies.
// The default behaviour (10 000 ms for ioredis, 5 000 ms for node-redis) is
// the root cause of the intermittent ~5-second timing flakes seen in the
// suite: when Vitest spins up several worker forks in parallel against the
// shared testcontainer Redis (~150 brand-new TCP connections in the same
// second, including the duplicate() pubsub subscriber on every limiter), the
// Docker-Desktop network proxy occasionally drops the *initial* SYN. The
// proxy retries transparently, but only on a 5-second cadence, so the
// affected client's `connect` event fires exactly ~5 000 ms after socket
// creation. Every Redis operation that a test queues against that client
// during start-up is then shifted by 5 s, and `checkDuration(250)` blows up
// with values like 5 259.
//
// Empirically, healthy connects against the testcontainer settle in well
// under 100 ms even under heavy parallel-worker load, while the pathological
// ones sit silent for ~5 000 ms with no intermediate events — a clean
// bimodal distribution. 500 ms is the lowest value that doesn't trip
// healthy connects under load (250 ms produced spurious retries that broke
// timing-sensitive tests in `priority.test.js` and `general-traffic.test.js`).
// Tests with a 500 ms upper bound that previously fit inside the heartbeat
// budget have been widened slightly to absorb a single timeout+retry cycle.
export default function buildClientOptions(datastore: string): Record<string, unknown> {
  const host = process.env.REDIS_HOST;
  const port = process.env.REDIS_PORT;
  if (datastore === "redis") {
    return {
      socket: {
        host,
        port: port ? Number(port) : undefined,
        connectTimeout: 500,
        reconnectStrategy: (retries: number) => Math.min(retries * 50, 500),
      },
    };
  }
  return {
    host,
    port: port ? Number(port) : undefined,
    connectTimeout: 500,
    retryStrategy: (times: number) => Math.min(times * 50, 500),
  };
}
