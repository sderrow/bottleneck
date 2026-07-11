import { test, describe, expect } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");
const Redis = require("redis");
const buildClientOptions = require("./redis-client-options");

describe("node_redis-only", () => {
  test("Should accept node_redis lib override", ({ makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 2,
      Redis,
    });

    expect(limiter.datastore).toStrictEqual("redis");
  });

  test("Should accept existing connections", ({ harness: h, makeLimiter, track }) => {
    const connection = track(
      new Bottleneck.RedisConnection({
        Redis,
        clientOptions: buildClientOptions("redis"),
      }),
    );
    connection.id = "super-connection";
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    expect(limiter.schedule(h.promise, null, 1)).resolves.toEqual([1]);
    expect(limiter.schedule(h.promise, null, 2)).resolves.toEqual([2]);

    return h
      .flushLimiter(limiter)
      .then((_results) => {
        expect(h.log).toHaveCallOrder([[1], [2]]);
        expect(h).toHaveFinalCallAt(50);
        expect(limiter.connection.id).toStrictEqual("super-connection");
        expect(limiter.datastore).toStrictEqual("redis");

        return limiter.disconnect();
      })
      .then(() => {
        expect(limiter.clients().client.isReady).toStrictEqual(true);
      });
  });

  test("Should accept existing redis clients", async ({ harness: h, makeLimiter, track }) => {
    const client = Redis.createClient(buildClientOptions("redis"));
    client.id = "super-client";
    await client.connect();

    const connection = track(new Bottleneck.RedisConnection({ client }));
    connection.id = "super-connection";
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    expect(limiter.schedule(h.promise, null, 1)).resolves.toEqual([1]);
    expect(limiter.schedule(h.promise, null, 2)).resolves.toEqual([2]);

    return h
      .flushLimiter(limiter)
      .then((_results) => {
        expect(h.log).toHaveCallOrder([[1], [2]]);
        expect(h).toHaveFinalCallAt(50);
        expect(limiter.clients().client.id).toStrictEqual("super-client");
        expect(limiter.connection.id).toStrictEqual("super-connection");
        expect(limiter.datastore).toStrictEqual("redis");

        return limiter.disconnect();
      })
      .then(() => {
        expect(limiter.clients().client.isReady).toStrictEqual(true);
      });
  });

  test("Should trigger error events on the shared connection", ({ makeLimiter, track }) => {
    expect.hasAssertions();
    return new Promise((resolve, reject) => {
      const connection = track(
        new Bottleneck.RedisConnection({
          Redis,
          clientOptions: {
            socket: {
              port: 1,
              reconnectStrategy: () => false,
            },
          },
        }),
      );
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
