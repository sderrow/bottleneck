import Redis from "ioredis";
import ioredisPkg from "ioredis/package.json" with { type: "json" };
import { describe, expect, onTestFinished, vi } from "vitest";
import { limiterKeys, redisStore } from "./helpers/store";
import { test } from "./helpers/test-api";
import buildClientOptions from "./redis-client-options";

describe("ioredis-only", () => {
  test("Should accept ioredis lib override", ({ makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 2,
      Redis,
      clientOptions: {},
      clusterNodes: [
        {
          host: process.env.REDIS_HOST,
          port: process.env.REDIS_PORT,
        },
      ],
    });

    expect(limiter.datastore).toStrictEqual("ioredis");
  });

  test("Should connect in Redis Cluster mode", ({ makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 2,
      clientOptions: {},
      clusterNodes: [
        {
          host: process.env.REDIS_HOST,
          port: process.env.REDIS_PORT,
        },
      ],
    });

    expect(limiter.datastore).toStrictEqual("ioredis");
    expect(redisStore(limiter).connection.client).toBeInstanceOf(Redis.Cluster);
  });

  test("Should connect in Redis Cluster mode with premade client", ({
    makeLimiter,
    makeConnection,
  }) => {
    // The test server isn't a Redis Cluster, so this only checks wiring: the
    // client never connects, its errors are expected, and with no offline
    // queue its commands fail fast instead of hanging teardown.
    const client = new Redis.Cluster(
      [{ host: process.env.REDIS_HOST, port: Number(process.env.REDIS_PORT) }],
      { enableOfflineQueue: false },
    );
    const connection = makeConnection({ client });
    const limiter = makeLimiter({ maxConcurrent: 2, connection }, { expectErrors: true });

    expect(limiter.datastore).toStrictEqual("ioredis");
    expect(limiter.connection).toBe(connection);
    expect(connection.client).toBe(client);
    // The subscriber is a second Cluster client, not the premade one.
    expect(connection.subscriber).toBeInstanceOf(Redis.Cluster);
    expect(connection.subscriber).not.toBe(client);
  });

  test("Should accept existing connections", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const connection = makeConnection();
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(50);
    expect(limiter.connection).toBe(connection);
    expect(limiter.datastore).toStrictEqual("ioredis");

    await limiter.disconnect();
    expect(limiter.clients().client).toHaveProperty("status", "ready");
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should accept existing redis clients", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const client = new Redis(buildClientOptions("ioredis"));
    onTestFinished(() => client.disconnect());

    const connection = makeConnection({ client });
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(50);
    expect(limiter.clients().client).toBe(client);
    expect(limiter.connection).toBe(connection);
    expect(limiter.datastore).toStrictEqual("ioredis");

    await limiter.disconnect();
    expect(limiter.clients().client).toHaveProperty("status", "ready");
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should trigger error events on the shared connection", ({
    makeLimiter,
    makeClient,
    makeConnection,
  }) => {
    expect.hasAssertions();
    return new Promise<void>((resolve, reject) => {
      const connection = makeConnection({
        client: makeClient({ port: 1 }, { expectErrors: true }),
      });
      let fired = false;
      const limiter = makeLimiter({ connection });
      connection.on("error", (_err: unknown) => {
        if (fired) return;
        fired = true;
        expect(limiter.datastore).toStrictEqual("ioredis");
        connection.disconnect();
        resolve();
      });

      limiter.on("error", (err) => {
        if (fired) return;
        reject(err);
      });
    });
  });
});

// RESP3 support (protocol negotiation + replyMapping) only exists in ioredis
// 6+. The CI client-matrix runs this suite against ioredis 5 as well, so the
// RESP3-specific tests are skipped there; the RESP2 test still applies since
// ioredis 5 ignores the unknown `protocol` option and is RESP2-only anyway.
const ioredisMajor = parseInt(ioredisPkg.version, 10);
const describeResp3 = ioredisMajor >= 6 ? describe : describe.skip;

// ioredis 6 negotiates RESP3 by default but keeps RESP2-compatible reply
// shapes via the default `replyMapping: "legacy"`. Opting into
// `replyMapping: "resp3"` changes reply shapes (maps as objects, doubles as
// numbers), which is what Bottleneck must normalize back to RESP2 form.
const resp3ClientOptions = () => ({
  ...buildClientOptions("ioredis"),
  protocol: 3,
  replyMapping: "resp3",
});

describeResp3("ioredis RESP3", () => {
  test("Should run jobs over RESP3 with resp3 reply mapping", async ({
    harness: h,
    makeLimiter,
  }) => {
    const limiter = makeLimiter({
      maxConcurrent: 2,
      minTime: 50,
      clientOptions: resp3ClientOptions(),
    });
    expect(limiter.datastore).toStrictEqual("ioredis");
    await limiter.ready();

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);
    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(50);
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should normalize RESP3 hgetall and withscores replies in __runCommand__", async ({
    makeClient,
    makeConnection,
  }) => {
    const connection = makeConnection({ client: makeClient(resp3ClientOptions()) });
    await connection.ready;
    // Guard against silently testing RESP2: if this ever fails on an ioredis
    // upgrade, the option name or default protocol changed and these tests
    // are no longer covering the RESP3 path.
    expect(connection.client).toHaveProperty("options.protocol", 3);
    expect(connection.client).toHaveProperty("options.replyMapping", "resp3");
    const prefix = process.env.BOTTLENECK_TEST_PREFIX;
    const hashKey = `b_${prefix}resp3-hash`;
    const zsetKey = `b_${prefix}resp3-zset`;

    await connection.__runCommand__(["hset", hashKey, "field", "value"]);
    const hash = await connection.__runCommand__(["hgetall", hashKey]);
    expect(hash).toStrictEqual({ field: "value" });

    await connection.__runCommand__(["zadd", zsetKey, "5", "member"]);
    const scores = await connection.__runCommand__(["zrange", zsetKey, "0", "-1", "withscores"]);
    expect(scores).toStrictEqual(["member", "5"]);

    const deleted = await connection.__runCommand__(["del", hashKey, zsetKey]);
    expect(deleted).toStrictEqual(2);
  });

  test("Should normalize RESP2 replies in __runCommand__ when protocol 2 is forced", async ({
    makeClient,
    makeConnection,
  }) => {
    // ioredis 5 (still a supported peer dep) is RESP2-only; force the same
    // wire protocol under ioredis 6 so the RESP2 branches of normalizeReply
    // (HGETALL flat array, WITHSCORES passthrough) run against a real server.
    const connection = makeConnection({
      client: makeClient({ ...buildClientOptions("ioredis"), protocol: 2 }),
    });
    await connection.ready;
    // ioredis 5 has no protocol option (RESP2-only); only assert under v6+.
    if (ioredisMajor >= 6) {
      expect(connection.client).toHaveProperty("options.protocol", 2);
    }
    const prefix = process.env.BOTTLENECK_TEST_PREFIX;
    const hashKey = `b_${prefix}resp2-hash`;
    const zsetKey = `b_${prefix}resp2-zset`;

    await connection.__runCommand__(["hset", hashKey, "field", "value"]);
    // RESP2 returns a flat array; normalizeReply converts it to an object.
    expect(await connection.__runCommand__(["hgetall", hashKey])).toStrictEqual({
      field: "value",
    });

    await connection.__runCommand__(["zadd", zsetKey, "5", "member"]);
    // RESP2 already returns the flat [member, "score", ...] form; passthrough.
    expect(
      await connection.__runCommand__(["zrange", zsetKey, "0", "-1", "withscores"]),
    ).toStrictEqual(["member", "5"]);

    expect(await connection.__runCommand__(["del", hashKey, zsetKey])).toStrictEqual(2);
  });

  test("Should support Group deleteKey and clusterKeys over RESP3", async ({
    harness: h,
    makeGroup,
  }) => {
    const group = makeGroup({
      datastore: "ioredis",
      clientOptions: resp3ClientOptions(),
    });

    await Promise.all([
      expect(group.key("AAA").schedule(h.promise, null, 1)).resolves.toEqual([1]),
      expect(group.key("BBB").schedule(h.promise, null, 2)).resolves.toEqual([2]),
    ]);

    expect((await group.clusterKeys()).sort()).toStrictEqual(["AAA", "BBB"]);

    expect(await group.deleteKey("AAA")).toStrictEqual(true);
    expect(await group.deleteKey("AAA")).toStrictEqual(false);
  });
});

// The client belongs to the consumer: the connection only sends commands on
// it, never adding listeners or script methods, and never closing it.
describe("ioredis passed-in client", () => {
  test("Should only send commands on a passed-in client", async ({
    makeLimiter,
    makeConnection,
  }) => {
    const client = new Redis(buildClientOptions("ioredis"));
    onTestFinished(() => client.disconnect());
    const consumerListener = vi.fn<() => void>();
    client.on("error", consumerListener);
    const maxListeners = client.getMaxListeners();

    const connection = makeConnection({ client });
    const limiter = makeLimiter({ connection });
    await expect(limiter.schedule(() => Promise.resolve("ran"))).resolves.toBe("ran");
    expect(client.listeners("error")).toEqual([consumerListener]);
    expect(client.getMaxListeners()).toBe(maxListeners);
    // Scripts run through EVALSHA/EVAL, not methods defined on the client.
    expect(client).not.toHaveProperty("submit");

    await limiter.disconnect();
    await connection.disconnect(true);
    expect(client.listeners("error")).toEqual([consumerListener]);
    expect(client.status).toBe("ready");
  });

  test("Should apply the client's keyPrefix to script keys", async ({
    makeClient,
    makeConnection,
    makeLimiter,
  }) => {
    // Inside the per-fork prefix, so test cleanup still finds these keys.
    const keyPrefix = `b_${process.env.BOTTLENECK_TEST_PREFIX}kp:`;
    const connection = makeConnection({
      client: makeClient({ ...buildClientOptions("ioredis"), keyPrefix }),
    });
    const limiter = makeLimiter({ id: "prefixed", connection });
    await expect(limiter.schedule(() => Promise.resolve("ran"))).resolves.toBe("ran");

    const [settingsKey] = limiterKeys(limiter);
    const unprefixed = makeConnection();
    expect(await unprefixed.__runCommand__(["exists", `${keyPrefix}${settingsKey}`])).toBe(1);
    expect(await unprefixed.__runCommand__(["exists", settingsKey])).toBe(0);
  });

  test("Should leave a passed-in client open on disconnect(false)", async ({ makeConnection }) => {
    const client = new Redis(buildClientOptions("ioredis"));
    onTestFinished(() => client.disconnect());
    const connection = makeConnection({ client });
    await connection.ready;

    await connection.disconnect(false);

    await expect(client.ping()).resolves.toBe("PONG");
  });
});
