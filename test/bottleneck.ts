import SourceBottleneck from "../src/index.ts";

type BottleneckClass = typeof SourceBottleneck;

// dist/ is generated on demand by the smoke-test globalSetup (`tsdown --filter
// ...`) and does not exist for a plain `pnpm tsc`, so the dist entries are
// imported via a non-literal specifier: dynamic import of a `string` yields
// `Promise<any>`, which tsc accepts without resolving and assigns without a
// cast. `/* @vite-ignore */` leaves the specifier untouched so Vitest performs
// a native runtime import once dist exists on disk.
const entry = process.env.BOTTLENECK_ENTRY ?? "source";
const distPath: string | null =
  entry === "light" ? "../dist/light.js" : entry === "lib" ? "../dist/index.cjs" : null;
const Bottleneck: BottleneckClass =
  distPath == null ? SourceBottleneck : (await import(/* @vite-ignore */ distPath)).default;

// Test-harness convention: `datastore: "redis" | "ioredis"` (and optionally
// `clientOptions`) asks for a Redis-backed limiter or Group with its own test
// client and connection, which the wrapper builds, owns, and closes on
// disconnect(). The product itself only takes `connection`. Options with a
// `connection` are Redis-backed too. Both paths must get the test heartbeat
// override applied, otherwise child limiters created from a
// "connection-only" Group inherit the 5000ms production default and produce
// 5-second flakes (see cluster.test.ts:75 and similar).
const isRedisBacked = (
  options: Record<string, unknown> | undefined,
): options is Record<string, unknown> =>
  options != null &&
  typeof options === "object" &&
  (options.datastore === "redis" || options.datastore === "ioredis" || options.connection != null);

const usingRedis = process.env.DATASTORE === "redis" || process.env.DATASTORE === "ioredis";

async function makeTestBottleneck(Base: BottleneckClass): Promise<BottleneckClass> {
  const { closeTestClient, makeTestClient } = await import("./helpers/clients.ts");
  type TestClient = ReturnType<typeof makeTestClient>;
  type Owned = { client: TestClient; connection: { disconnect(flush?: boolean): Promise<void> } };

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

  // The test client's clientOptions default to buildClientOptions(datastore):
  // without them, ioredis/node-redis default to localhost:6379, which can
  // silently point at a different Redis than the one the test harness
  // provisioned (e.g. a leftover Docker container, dev server, or host Redis),
  // producing tests that "pass" by accidentally writing to two databases.
  // The shape is datastore-specific: ioredis accepts flat { host, port }, but
  // node-redis v4/v5 requires { socket: { host, port } } and silently ignores
  // top-level host/port (defaulting to localhost:6379).
  const buildClientOptions = (await import("./redis-client-options.ts")).default;

  const ownConnection = (datastore: unknown, clientOptions: unknown): Owned => {
    const options =
      (clientOptions as Record<string, unknown> | undefined) ??
      buildClientOptions(String(datastore));
    // The client's errors surface through the limiter (failed commands, and
    // the connection's subscriber errors), so the harness client stays quiet.
    const client = makeTestClient(options, { expectErrors: true, datastore: String(datastore) });
    // Only ioredis clients have a `status`.
    const connection =
      "status" in client
        ? new Base.IORedisConnection({ client })
        : new Base.RedisConnection({ client });
    return { client, connection };
  };

  const withRedis = (options: Record<string, unknown> | undefined) => {
    if (!isRedisBacked(options)) return { options, owned: null };
    const { datastore, clientOptions, ...next } = options;
    let owned: Owned | null = null;
    if (next.connection == null) {
      owned = ownConnection(datastore, clientOptions);
      next.connection = owned.connection;
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
    // (e.g. heartbeatInterval: 75 in general.test.ts auto-refresh tests).
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
    if (typeof options.id === "string" && options.id.startsWith(FILE_PREFIX)) {
      next.id = options.id;
    } else {
      next.id = FILE_PREFIX + String(options.id ?? "no-id");
    }
    return { options: next, owned };
  };

  const closeOwned = async (owned: Owned | null, flush?: boolean) => {
    if (owned == null) return;
    await owned.connection.disconnect(flush);
    await closeTestClient(owned.client);
  };

  // `Group.limiters()` returns instances of the real `Bottleneck`, so we override
  // `Symbol.hasInstance` to keep `instanceof` checks in tests behaving as expected.
  class TestBottleneck extends Base {
    static override [Symbol.hasInstance](instance: unknown) {
      return instance instanceof Base;
    }
    #owned: Owned | null;
    constructor(options?: Record<string, unknown>) {
      const { options: next, owned } = withRedis(options);
      super(next);
      this.#owned = owned;
    }
    override async disconnect(flush?: boolean): Promise<void> {
      await super.disconnect(flush);
      await closeOwned(this.#owned, flush);
      this.#owned = null;
    }
  }

  TestBottleneck.Group = class TestGroup extends Base.Group {
    static override [Symbol.hasInstance](instance: unknown) {
      return instance instanceof Base.Group;
    }
    #owned: Owned | null;
    constructor(options?: Record<string, unknown>) {
      const { options: next, owned } = withRedis(options);
      super(next);
      this.#owned = owned;
      // Group.key() instantiates child limiters via `this.Bottleneck`, which
      // the parent Group constructor sets to the library's Bottleneck class.
      // That bypasses our test-wrapper's heartbeatInterval injection, so
      // child limiters fall back to the 5000ms production default and
      // produce the exact 5-second flakes we're trying to eliminate. Pointing
      // it at TestBottleneck makes child limiters inherit the test wrapper.
      this.Bottleneck = TestBottleneck;
    }
    override async disconnect(flush?: boolean): Promise<void> {
      await super.disconnect(flush);
      await closeOwned(this.#owned, flush);
      this.#owned = null;
    }
  };

  return TestBottleneck;
}

export default usingRedis ? await makeTestBottleneck(Bottleneck) : Bottleneck;
