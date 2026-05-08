import { describe, it, afterEach, expect } from "vitest";
import { createJobHarness } from "./helpers/job-tracking.js";
const makeLimiter = require("./helpers/limiter");
const Bottleneck = require("./bottleneck");
const Redis = require("ioredis");
const buildClientOptions = require("./redis-client-options");

describe("ioredis-only", function () {
  if (process.env.DATASTORE !== "ioredis") {
    throw new Error("DATASTORE must be ioredis");
  }
  let limiter;

  afterEach(function () {
    return limiter.disconnect(false);
  });

  it("Should accept ioredis lib override", function () {
    limiter = makeLimiter({
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

  it("Should connect in Redis Cluster mode", function () {
    limiter = makeLimiter({
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

  it("Should connect in Redis Cluster mode with premade client", function () {
    const client = new Redis.Cluster("");
    const connection = new Bottleneck.IORedisConnection({ client });
    limiter = makeLimiter({
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
    connection.disconnect(false);
  });

  it("Should accept existing connections", function () {
    const h = createJobHarness();
    const connection = new Bottleneck.IORedisConnection({
      Redis,
      clientOptions: buildClientOptions("ioredis"),
    });
    connection.id = "super-connection";
    limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    h.pNoErrVal(limiter.schedule(h.promise, null, 1), 1);
    h.pNoErrVal(limiter.schedule(h.promise, null, 2), 2);

    return h
      .flushLimiter(limiter)
      .then(function (_results) {
        h.checkResultsOrder([[1], [2]]);
        h.checkDuration(50);
        expect(limiter.connection.id).toStrictEqual("super-connection");
        expect(limiter.datastore).toStrictEqual("ioredis");

        return limiter.disconnect();
      })
      .then(function () {
        expect(limiter.clients().client.status).toStrictEqual("ready");
        return connection.disconnect();
      });
  });

  it("Should accept existing redis clients", function () {
    const h = createJobHarness();
    const client = new Redis(buildClientOptions("ioredis"));
    client.id = "super-client";

    const connection = new Bottleneck.IORedisConnection({ client });
    connection.id = "super-connection";
    limiter = makeLimiter({
      minTime: 50,
      connection,
    });

    h.pNoErrVal(limiter.schedule(h.promise, null, 1), 1);
    h.pNoErrVal(limiter.schedule(h.promise, null, 2), 2);

    return h
      .flushLimiter(limiter)
      .then(function (_results) {
        h.checkResultsOrder([[1], [2]]);
        h.checkDuration(50);
        expect(limiter.clients().client.id).toStrictEqual("super-client");
        expect(limiter.connection.id).toStrictEqual("super-connection");
        expect(limiter.datastore).toStrictEqual("ioredis");

        return limiter.disconnect();
      })
      .then(function () {
        expect(limiter.clients().client.status).toStrictEqual("ready");
        return connection.disconnect();
      });
  });

  it("Should trigger error events on the shared connection", function () {
    expect.hasAssertions();
    return new Promise(function (resolve, reject) {
      const connection = new Bottleneck.IORedisConnection({
        Redis,
        clientOptions: {
          port: 1,
        },
      });
      let fired = false;
      limiter = makeLimiter({ connection });
      connection.on("error", function (_err) {
        if (fired) return;
        fired = true;
        expect(limiter.datastore).toStrictEqual("ioredis");
        connection.disconnect();
        resolve();
      });

      limiter.on("error", function (err) {
        if (fired) return;
        reject(err);
      });
    });
  });
});
