import { describe, expect } from "vitest";
import { test } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");
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

  test("Should connect in Redis Cluster mode with premade client", ({ makeLimiter, track }) => {
    const client = new Redis.Cluster("");
    track(new Bottleneck.IORedisConnection({ client }));
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

  test("Should accept existing connections", ({ harness: h, makeLimiter, track }) => {
    const connection = track(
      new Bottleneck.IORedisConnection({
        Redis,
        clientOptions: buildClientOptions("ioredis"),
      }),
    );
    connection.id = "super-connection";
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    return h
      .flushLimiter(limiter)
      .then((_results) => {
        expect(h.log).toHaveCallOrder([[1], [2]]);
        expect(h).toHaveFinalCallAt(50);
        expect(limiter.connection.id).toStrictEqual("super-connection");
        expect(limiter.datastore).toStrictEqual("ioredis");

        return limiter.disconnect();
      })
      .then(() => {
        expect(limiter.clients().client.status).toStrictEqual("ready");
        return Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
      });
  });

  test("Should accept existing redis clients", ({ harness: h, makeLimiter, track }) => {
    const client = new Redis(buildClientOptions("ioredis"));
    client.id = "super-client";

    const connection = track(new Bottleneck.IORedisConnection({ client }));
    connection.id = "super-connection";
    const limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    const p1 = limiter.schedule(h.promise, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);

    return h
      .flushLimiter(limiter)
      .then((_results) => {
        expect(h.log).toHaveCallOrder([[1], [2]]);
        expect(h).toHaveFinalCallAt(50);
        expect(limiter.clients().client.id).toStrictEqual("super-client");
        expect(limiter.connection.id).toStrictEqual("super-connection");
        expect(limiter.datastore).toStrictEqual("ioredis");

        return limiter.disconnect();
      })
      .then(() => {
        expect(limiter.clients().client.status).toStrictEqual("ready");
        return Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
      });
  });

  test("Should trigger error events on the shared connection", ({ makeLimiter, track }) => {
    expect.hasAssertions();
    return new Promise((resolve, reject) => {
      const connection = track(
        new Bottleneck.IORedisConnection({
          Redis,
          clientOptions: {
            port: 1,
          },
        }),
      );
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
