import { describe, expect } from "vitest";
import { test } from "./helpers/test-api.js";
const Redis = require("redis");
import buildClientOptions from "./redis-client-options";

describe("node_redis-only", () => {
  test("Should accept node_redis lib override", ({ makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 2,
      Redis,
    });

    expect(limiter.datastore).toStrictEqual("redis");
  });

  test("Should accept existing connections", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const connection = makeConnection({
      Redis,
      clientOptions: buildClientOptions("redis"),
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
    expect(limiter.datastore).toStrictEqual("redis");

    await limiter.disconnect();
    expect(limiter.clients().client.isReady).toStrictEqual(true);
    await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
  });

  test("Should accept existing redis clients", async ({
    harness: h,
    makeLimiter,
    makeConnection,
  }) => {
    const client = Redis.createClient(buildClientOptions("redis"));
    client.id = "super-client";
    await client.connect();

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
    expect(limiter.datastore).toStrictEqual("redis");

    await limiter.disconnect();
    expect(limiter.clients().client.isReady).toStrictEqual(true);
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
          socket: {
            port: 1,
            reconnectStrategy: () => false,
          },
        },
      });
      connection.ready.catch(() => {});
      let fired = false;
      const limiter = makeLimiter({ connection }, { expectErrors: true });
      connection.on("error", (_err) => {
        if (fired) return;
        fired = true;
        expect(limiter.datastore).toStrictEqual("redis");
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
