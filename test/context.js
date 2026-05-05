global.TEST = true;
const assert = require("assert");
var Bottleneck = require("./bottleneck");

module.exports = function (options = {}) {
  var mustEqual = function (a, b) {
    assert.deepStrictEqual(a, b);
  };

  var start;
  var calls = [];

  // set options.datastore
  var setRedisClientOptions = function (options) {
    options.clearDatastore = true;
    if (options.clientOptions == null) {
      options.clientOptions = {
        host: process.env.REDIS_HOST,
        port: process.env.REDIS_PORT,
      };
    }
  };

  if (options.datastore == null && process.env.DATASTORE === "redis") {
    options.datastore = "redis";
    setRedisClientOptions(options);
  } else if (options.datastore == null && process.env.DATASTORE === "ioredis") {
    options.datastore = "ioredis";
    setRedisClientOptions(options);
  } else {
    options.datastore = "local";
  }

  var limiter = new Bottleneck(options);
  // limiter.on("debug", function (str, args) { console.log(`${Date.now()-start} ${str} ${JSON.stringify(args)}`) })
  if (!options.errorEventsExpected) {
    limiter.on("error", function (err) {
      console.log("(CONTEXT) ERROR EVENT", err);
    });
  }
  limiter.ready().then(function (_client) {
    start = Date.now();
  });
  var getResults = function () {
    return {
      elapsed: Date.now() - start,
      callsDuration: calls.length > 0 ? calls[calls.length - 1].time : null,
      calls: calls,
    };
  };

  var context = {
    job: function (err, ...result) {
      var cb = result.pop();
      calls.push({ err: err, result: result, time: Date.now() - start });
      if (process.env.DEBUG) console.log(result, calls);
      cb.apply({}, [err].concat(result));
    },
    slowJob: function (duration, err, ...result) {
      setTimeout(function () {
        var cb = result.pop();
        calls.push({ err: err, result: result, time: Date.now() - start });
        if (process.env.DEBUG) console.log(result, calls);
        cb.apply({}, [err].concat(result));
      }, duration);
    },
    promise: function (err, ...result) {
      return new Promise(function (resolve, reject) {
        if (process.env.DEBUG) console.log("In c.promise. Result: ", result);
        calls.push({ err: err, result: result, time: Date.now() - start });
        if (process.env.DEBUG) console.log(result, calls);
        if (err === null) {
          return resolve(result);
        } else {
          return reject(err);
        }
      });
    },
    slowPromise: function (duration, err, ...result) {
      return new Promise(function (resolve, reject) {
        setTimeout(function () {
          if (process.env.DEBUG) console.log("In c.slowPromise. Result: ", result);
          calls.push({ err: err, result: result, time: Date.now() - start });
          if (process.env.DEBUG) console.log(result, calls);
          if (err === null) {
            return resolve(result);
          } else {
            return reject(err);
          }
        }, duration);
      });
    },
    pNoErrVal: function (promise, ...expected) {
      if (process.env.DEBUG) console.log("In c.pNoErrVal. Expected:", expected);
      return promise.then(function (actual) {
        mustEqual(actual, expected);
      });
    },
    noErrVal: function (...expected) {
      return function (err, ...actual) {
        mustEqual(err, null);
        mustEqual(actual, expected);
      };
    },
    last: function (options) {
      var opt = options != null ? options : {};
      return limiter
        .schedule(opt, function () {
          return Promise.resolve(getResults());
        })
        .catch(function (err) {
          console.error("Error in context.last:", err);
        });
    },
    wait: function (wait) {
      return new Promise(function (resolve, _reject) {
        setTimeout(resolve, wait);
      });
    },
    limiter: limiter,
    mustEqual: mustEqual,
    mustGte: function (a, b) {
      assert(a >= b, `Expected ${a} to be greater than or equal to ${b}`);
    },
    mustGt: function (a, b) {
      assert(a > b, `Expected ${a} to be greater than ${b}`);
    },
    mustLte: function (a, b) {
      assert(a <= b, `Expected ${a} to be less than or equal to ${b}`);
    },
    mustLt: function (a, b) {
      assert(a < b, `Expected ${a} to be less than ${b}`);
    },
    mustExist: function (a) {
      assert(a != null, `Expected ${a} to exist`);
    },
    mustNotExist: function (a) {
      assert(a == null, `Expected ${a} to not exist`);
    },
    results: getResults,
    checkResultsOrder: function (order) {
      assert.deepStrictEqual(
        order.length,
        calls.length,
        `Expected ${order.length} calls, got ${calls.length} calls`,
      );
      for (var i = 0; i < calls.length; i++) {
        assert.deepStrictEqual(
          order[i],
          calls[i].result,
          `Expected ${order[i]} for call ${i}, got ${calls[i].result} instead`,
        );
      }
    },
    checkDuration: function (shouldBe, minBound = 10) {
      var results = getResults();
      var min = shouldBe - minBound;
      var max = shouldBe + 100;
      assert(
        results.callsDuration > min && results.callsDuration < max,
        `Expected ${results.callsDuration} to be around ${shouldBe} (between ${min} and ${max})`,
      );
    },
  };

  return context;
};
