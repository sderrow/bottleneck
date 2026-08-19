import { describe, expect } from "vitest";
import { test } from "./helpers/test-api.js";
const Redis = require("ioredis");
const buildClientOptions = require("./redis-client-options");

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
    expect(limiter._store.connection.client.nodes().length).toBeGreaterThanOrEqual(0);
  });

  test("Should connect in Redis Cluster mode with premade client", ({
    makeLimiter,
    makeConnection,
  }) => {
    const client = new Redis.Cluster("");
    makeConnection({ client });
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
    expect(limiter._store.connection.client.nodes().length).toBeGreaterThanOrEqual(0);
  });

  test("Should accept existing connections", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const connection = makeConnection({
      Redis,
      clientOptions: buildClientOptions("ioredis"),
    });
    connection.id = "super-connection";
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(50);
    expect(limiter.connection.id).toStrictEqual("super-connection");
    expect(limiter.datastore).toStrictEqual("ioredis");

    await limiter.disconnect();
    expect(limiter.clients().client.status).toStrictEqual("ready");
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should accept existing redis clients", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const client = new Redis(buildClientOptions("ioredis"));
    client.id = "super-client";

    const connection = makeConnection({ client });
    connection.id = "super-connection";
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(50);
    expect(limiter.clients().client.id).toStrictEqual("super-client");
    expect(limiter.connection.id).toStrictEqual("super-connection");
    expect(limiter.datastore).toStrictEqual("ioredis");

    await limiter.disconnect();
    expect(limiter.clients().client.status).toStrictEqual("ready");
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should trigger error events on the shared connection", ({
    makeLimiter,
    makeConnection,
  }) => {
    expect.hasAssertions();
    return new Promise((resolve, reject) => {
      const connection = makeConnection({
        Redis,
        clientOptions: {
          port: 1,
        },
      });
      let fired = false;
      const limiter = makeLimiter({ connection });
      connection.on("error", (_err) => {
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
const ioredisMajor = parseInt(Redis.version ?? require("ioredis/package.json").version, 10);
const describeResp3 = ioredisMajor >= 6 ? describe : describe.skip;

describeResp3("ioredis RESP3", () => {
  // ioredis 6 negotiates RESP3 by default but keeps RESP2-compatible reply
  // shapes via the default `replyMapping: "legacy"`. Opting into
  // `replyMapping: "resp3"` changes reply shapes (maps as objects, doubles as
  // numbers), which is what Bottleneck must normalize back to RESP2 form.
  const resp3ClientOptions = () => ({
    ...buildClientOptions("ioredis"),
    protocol: 3,
    replyMapping: "resp3",
  });

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
    makeConnection,
  }) => {
    const connection = makeConnection({ Redis, clientOptions: resp3ClientOptions() });
    await connection.ready;
    // Guard against silently testing RESP2: if this ever fails on an ioredis
    // upgrade, the option name or default protocol changed and these tests
    // are no longer covering the RESP3 path.
    expect(connection.client.options.protocol).toStrictEqual(3);
    expect(connection.client.options.replyMapping).toStrictEqual("resp3");
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
    makeConnection,
  }) => {
    // ioredis 5 (still a supported peer dep) is RESP2-only; force the same
    // wire protocol under ioredis 6 so the RESP2 branches of normalizeReply
    // (HGETALL flat array, WITHSCORES passthrough) run against a real server.
    const connection = makeConnection({
      Redis,
      clientOptions: { ...buildClientOptions("ioredis"), protocol: 2 },
    });
    await connection.ready;
    // ioredis 5 has no protocol option (RESP2-only); only assert under v6+.
    if (ioredisMajor >= 6) {
      expect(connection.client.options.protocol).toStrictEqual(2);
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
