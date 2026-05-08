import { describe, it, afterEach, expect } from "vitest";
import { createJobHarness } from "./helpers/job-tracking.js";
const makeLimiter = require("./helpers/limiter");
const Bottleneck = require("./bottleneck");
const Redis = require("redis");
const buildClientOptions = require("./redis-client-options");

describe("node_redis-only", function () {
  if (process.env.DATASTORE !== "redis") {
    throw new Error("DATASTORE must be redis");
  }
  let limiter;

  afterEach(function () {
    return limiter.disconnect(false);
  });

  it("Should accept node_redis lib override", function () {
    limiter = makeLimiter({
      maxConcurrent: 2,
      Redis,
    });

    expect(limiter.datastore).toStrictEqual("redis");
  });

  it("Should accept existing connections", function () {
    const h = createJobHarness();
    const connection = new Bottleneck.RedisConnection({
      Redis,
      clientOptions: buildClientOptions("redis"),
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
        expect(limiter.datastore).toStrictEqual("redis");

        return limiter.disconnect();
      })
      .then(function () {
        expect(limiter.clients().client.isReady).toStrictEqual(true);
        return connection.disconnect();
      });
  });

  it("Should accept existing redis clients", async function () {
    const h = createJobHarness();
    const client = Redis.createClient(buildClientOptions("redis"));
    client.id = "super-client";
    await client.connect();

    const connection = new Bottleneck.RedisConnection({ client });
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
        expect(limiter.datastore).toStrictEqual("redis");

        return limiter.disconnect();
      })
      .then(function () {
        expect(limiter.clients().client.isReady).toStrictEqual(true);
        return connection.disconnect();
      });
  });

  it("Should trigger error events on the shared connection", function () {
    expect.hasAssertions();
    return new Promise(function (resolve, reject) {
      const connection = new Bottleneck.RedisConnection({
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
      limiter = makeLimiter({ connection }, { expectErrors: true });
      connection.on("error", function (_err) {
        if (fired) return;
        fired = true;
        expect(limiter.datastore).toStrictEqual("redis");
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
