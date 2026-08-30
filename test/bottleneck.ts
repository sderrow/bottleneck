const resolveEntry = async () => {
  switch (process.env.BOTTLENECK_ENTRY ?? "source") {
    case "light":
      return (await import("../dist/light.js")).default;
    case "lib":
      return (await import("../dist/index.cjs")).default;
    default:
      return (await import("../src/index.ts")).default;
  }
};
const Bottleneck = await resolveEntry();

// A limiter (or group) is Redis-backed if its options either name a Redis
// datastore explicitly OR provide a pre-built `connection` (in which case
// the Bottleneck constructor infers the datastore from the connection).
// Both paths must get the test heartbeat override applied, otherwise
// child limiters created from a "connection-only" Group inherit the 5000ms
// production default and produce 5-second flakes (see cluster.test.js:75
// and similar).
const isRedisBacked = (options) =>
  options != null &&
  typeof options === "object" &&
  (options.datastore === "redis" || options.datastore === "ioredis" || options.connection != null);

const usingRedis = process.env.DATASTORE === "redis" || process.env.DATASTORE === "ioredis";

let ExportedBottleneck;

if (!usingRedis) {
  ExportedBottleneck = Bottleneck;
} else {
  const Redis =
    process.env.DATASTORE === "redis"
      ? (await import("redis")).default
      : (await import("ioredis")).default;

  // One prefix per fork. Every test limiter's `id` is namespaced with this so
  // workers running in parallel against the single shared Redis (started by
  // test/global-setup/redis.ts) never collide. Tests that pass the same `id` to two
  // limiters in the same fork still coordinate because both sides receive the
  // same prefix. The prefix is generated in test/setup.ts (which runs first per
  // fork) and forwarded via process.env so this module — which Vitest may
  // resolve via separate ESM and CJS module instances — observes the same value
  // regardless of load order. test/setup.ts uses the same value to build the
  // SCAN MATCH pattern for prefix-scoped cleanup between tests.
  if (!process.env.BOTTLENECK_TEST_PREFIX) {
    process.env.BOTTLENECK_TEST_PREFIX = `t-${process.pid}-${Math.random().toString(36).slice(2, 8)}-`;
  }
  const FILE_PREFIX = process.env.BOTTLENECK_TEST_PREFIX;

  // Tests construct limiters and Groups directly via `new Bottleneck({ datastore: ... })`
  // without threading `Redis` or `clientOptions` through every call site. We subclass
  // here to inject both. clientOptions is critical — without it, ioredis/node-redis
  // default to localhost:6379, which can silently point at a different Redis than the
  // one the test harness provisioned (e.g. a leftover Docker container, dev server, or
  // host Redis), producing tests that "pass" by accidentally writing to two databases.
  // The shape is datastore-specific: ioredis accepts flat { host, port }, but
  // node-redis v4/v5 requires { socket: { host, port } } and silently ignores
  // top-level host/port (defaulting to localhost:6379).
  const buildClientOptions = (await import("./redis-client-options.js")).default;

  const withRedis = (options) => {
    if (!isRedisBacked(options)) return options;
    const next = { ...options };
    // Only inject the Redis library / clientOptions when the test isn't
    // bringing its own pre-built client or connection. Both of those carry
    // their own clientOptions and the Bottleneck constructor would reject
    // duplicates.
    if (options.connection == null && options.client == null) {
      if (next.Redis == null) next.Redis = Redis;
      if (next.clientOptions == null) {
        next.clientOptions = buildClientOptions(next.datastore);
      }
    }
    // The default `heartbeatInterval` for Redis-backed limiters is 5000ms,
    // which bounds cross-limiter capacity-message recovery: when a
    // capacity-priority pubsub message races a limiter's submit and is
    // missed, the next heartbeat tick is what wakes the queued job up.
    // Under heavy parallel testcontainer load, that race opens up and
    // produces flakes that overshoot expected durations by ~5000ms. A
    // 250ms heartbeat closes the worst-case recovery window without
    // affecting tests that don't depend on heartbeat timing. Tests that
    // DO depend on heartbeat timing already set their own value explicitly
    // (e.g. heartbeatInterval: 75 in general.test.js auto-refresh tests).
    if (next.heartbeatInterval == null) next.heartbeatInterval = 250;
    // Namespace every limiter id with the fork-local FILE_PREFIX so this fork's
    // Redis keys don't collide with keys from any other parallel fork on the
    // shared Redis instance. Tests that intentionally share an `id` across two
    // limiters still coordinate (both limiters live in the same fork and both
    // get the same prefix prepended). Tests that introspect Redis keys do so
    // via `limiter._store.originalId`, which already reflects the prefixed id.
    //
    // Skip when the id is already prefixed: TestGroup prefixes its own id, and
    // Group.key() builds child ids as `${groupId}-${key}`, so child limiters
    // arrive here pre-prefixed. Re-prefixing them would break Group.clusterKeys
    // (which SCANs `b_${this.id}-*`).
    if (options.id != null && options.id.startsWith(FILE_PREFIX)) {
      next.id = options.id;
    } else {
      next.id = FILE_PREFIX + (options.id ?? "no-id");
    }
    return next;
  };

  // `Group.limiters()` returns instances of the real `Bottleneck`, so we override
  // `Symbol.hasInstance` to keep `instanceof` checks in tests behaving as expected.
  class TestBottleneck extends Bottleneck {
    static [Symbol.hasInstance](instance) {
      return instance instanceof Bottleneck;
    }
    constructor(options) {
      super(withRedis(options));
    }
  }

  TestBottleneck.Group = class TestGroup extends Bottleneck.Group {
    static [Symbol.hasInstance](instance) {
      return instance instanceof Bottleneck.Group;
    }
    constructor(options) {
      super(withRedis(options));
      // Group.key() instantiates child limiters via `this.Bottleneck`, which
      // the parent Group constructor sets to the library's Bottleneck class.
      // That bypasses our test-wrapper's heartbeatInterval injection, so
      // child limiters fall back to the 5000ms production default and
      // produce the exact 5-second flakes we're trying to eliminate. Pointing
      // it at TestBottleneck makes child limiters inherit the test wrapper.
      this.Bottleneck = TestBottleneck;
    }
  };

  ExportedBottleneck = TestBottleneck;
}

export default ExportedBottleneck;
