var makeTest = require("./context");
var Bottleneck = require("./bottleneck");
const { describe, it, afterEach } = require("mocha");
var Redis = require("redis");

if (process.env.DATASTORE === "redis") {
  describe("node_redis-only", function () {
    var c;

    afterEach(function () {
      return c.limiter.disconnect(false);
    });

    it("Should accept node_redis lib override", function () {
      c = makeTest({
        maxConcurrent: 2,
        Redis,
        clientOptions: {},
      });

      c.mustEqual(c.limiter.datastore, "redis");
    });

    it("Should accept existing connections", function () {
      var connection = new Bottleneck.RedisConnection({ Redis });
      connection.id = "super-connection";
      c = makeTest({
        minTime: 50,
        connection,
      });

      c.pNoErrVal(c.limiter.schedule(c.promise, null, 1), 1);
      c.pNoErrVal(c.limiter.schedule(c.promise, null, 2), 2);

      return c
        .last()
        .then(function (_results) {
          c.checkResultsOrder([[1], [2]]);
          c.checkDuration(50);
          c.mustEqual(c.limiter.connection.id, "super-connection");
          c.mustEqual(c.limiter.datastore, "redis");

          return c.limiter.disconnect();
        })
        .then(function () {
          // Shared connections should not be disconnected by the limiter
          c.mustEqual(c.limiter.clients().client.isReady, true);
          return connection.disconnect();
        });
    });

    it("Should accept existing redis clients", async function () {
      var client = Redis.createClient({
        socket: {
          host: process.env.REDIS_HOST,
          port: process.env.REDIS_PORT ? Number(process.env.REDIS_PORT) : undefined,
        },
      });
      client.id = "super-client";
      await client.connect();

      var connection = new Bottleneck.RedisConnection({ client });
      connection.id = "super-connection";
      c = makeTest({
        minTime: 50,
        connection,
      });

      c.pNoErrVal(c.limiter.schedule(c.promise, null, 1), 1);
      c.pNoErrVal(c.limiter.schedule(c.promise, null, 2), 2);

      return c
        .last()
        .then(function (_results) {
          c.checkResultsOrder([[1], [2]]);
          c.checkDuration(50);
          c.mustEqual(c.limiter.clients().client.id, "super-client");
          c.mustEqual(c.limiter.connection.id, "super-connection");
          c.mustEqual(c.limiter.datastore, "redis");

          return c.limiter.disconnect();
        })
        .then(function () {
          // Shared connections should not be disconnected by the limiter
          c.mustEqual(c.limiter.clients().client.isReady, true);
          return connection.disconnect();
        });
    });

    it("Should trigger error events on the shared connection", function (done) {
      var connection = new Bottleneck.RedisConnection({
        Redis,
        clientOptions: {
          socket: {
            port: 1,
            reconnectStrategy: () => false,
          },
        },
      });
      var fired = false;
      connection.on("error", function (_err) {
        if (fired) return;
        fired = true;
        c.mustEqual(c.limiter.datastore, "redis");
        connection.disconnect();
        done();
      });

      c = makeTest({ connection, errorEventsExpected: true });
      c.limiter.on("error", function (err) {
        if (fired) return;
        done(err);
      });
    });
  });
}
