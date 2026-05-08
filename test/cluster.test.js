import { describe, it, afterEach, expect } from "vitest";
import { createJobHarness } from "./helpers/job-tracking.js";
import { waitForState } from "./helpers/wait-for-state.js";
const makeLimiter = require("./helpers/limiter");
const Bottleneck = require("./bottleneck");
const Scripts = require("../src/cluster/Scripts.js");
const assert = require("assert");

const limiterKeys = function (limiter) {
  return Scripts.allKeys(limiter._store.originalId);
};
const runCommand = function (limiter, command, args) {
  return limiter._store.connection.__runCommand__([command, ...args]);
};
const sumWeights = function (weights) {
  return Object.keys(weights).reduce((acc, x) => {
    return acc + ~~weights[x];
  }, 0);
};

describe("Cluster-only", function () {
  if (process.env.DATASTORE !== "redis" && process.env.DATASTORE !== "ioredis") {
    throw new Error("DATASTORE must be redis or ioredis");
  }
  let rootLimiter;

  afterEach(function () {
    return rootLimiter.disconnect(false);
  });

  it("Should return a promise for ready()", function () {
    rootLimiter = makeLimiter({ maxConcurrent: 2 });

    const ready = rootLimiter.ready();
    expect(ready).toBeInstanceOf(Promise);
    return ready;
  });

  it("Should return clients", function () {
    rootLimiter = makeLimiter({ maxConcurrent: 2 });

    return rootLimiter.ready().then(function (clients) {
      expect(Object.keys(clients)).toEqual(["client", "subscriber"]);
      expect(Object.keys(rootLimiter.clients())).toEqual(["client", "subscriber"]);
    });
  });

  it("Should return a promise when disconnecting", function () {
    rootLimiter = makeLimiter({ maxConcurrent: 2 });

    const disconnected = rootLimiter.disconnect();
    expect(disconnected).toBeInstanceOf(Promise);
    return disconnected.then(function () {
      // do nothing
    });
  });

  it("Should allow passing a limiter's connection to a new limiter", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter();
    rootLimiter.connection.id = "some-id";
    const limiter = new Bottleneck({
      minTime: 50,
      connection: rootLimiter.connection,
    });

    return Promise.all([rootLimiter.ready(), limiter.ready()])
      .then(function () {
        expect(limiter.connection.id).toEqual("some-id");
        expect(limiter.datastore).toEqual(process.env.DATASTORE);

        return Promise.all([
          h.pNoErrVal(rootLimiter.schedule(h.promise, null, 1), 1),
          h.pNoErrVal(limiter.schedule(h.promise, null, 2), 2),
        ]);
      })
      .then(function () {
        return h.flushLimiter(rootLimiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[1], [2]]);
      });
  });

  it("Should allow passing a limiter's connection to a new Group", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter();
    rootLimiter.connection.id = "some-id";
    const group = new Bottleneck.Group({
      minTime: 50,
      connection: rootLimiter.connection,
    });
    const limiter1 = group.key("A");
    const limiter2 = group.key("B");

    return Promise.all([rootLimiter.ready(), limiter1.ready(), limiter2.ready()])
      .then(function () {
        expect(limiter1.connection.id).toEqual("some-id");
        expect(limiter2.connection.id).toEqual("some-id");
        expect(limiter1.datastore).toEqual(process.env.DATASTORE);
        expect(limiter2.datastore).toEqual(process.env.DATASTORE);

        return Promise.all([
          h.pNoErrVal(rootLimiter.schedule(h.promise, null, 1), 1),
          h.pNoErrVal(limiter1.schedule(h.promise, null, 2), 2),
          h.pNoErrVal(limiter2.schedule(h.promise, null, 3), 3),
        ]);
      })
      .then(function () {
        return h.flushLimiter(rootLimiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[1], [2], [3]]);
      });
  });

  it("Should allow passing a Group's connection to a new limiter", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter();
    const group = new Bottleneck.Group({
      minTime: 50,
      datastore: process.env.DATASTORE,
      clearDatastore: true,
    });
    group.connection.id = "some-id";

    const limiter1 = group.key("A");
    const limiter2 = new Bottleneck({
      minTime: 50,
      connection: group.connection,
    });

    return Promise.all([limiter1.ready(), limiter2.ready()])
      .then(function () {
        expect(limiter1.connection.id).toEqual("some-id");
        expect(limiter2.connection.id).toEqual("some-id");
        expect(limiter1.datastore).toEqual(process.env.DATASTORE);
        expect(limiter2.datastore).toEqual(process.env.DATASTORE);

        return Promise.all([
          h.pNoErrVal(limiter1.schedule(h.promise, null, 1), 1),
          h.pNoErrVal(limiter2.schedule(h.promise, null, 2), 2),
        ]);
      })
      .then(function () {
        return h.flushLimiter(rootLimiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[1], [2]]);
        return group.disconnect();
      });
  });

  it("Should allow passing a Group's connection to a new Group", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter();
    const group1 = new Bottleneck.Group({
      minTime: 50,
      datastore: process.env.DATASTORE,
      clearDatastore: true,
    });
    group1.connection.id = "some-id";

    const group2 = new Bottleneck.Group({
      minTime: 50,
      connection: group1.connection,
      clearDatastore: true,
    });

    const limiter1 = group1.key("AAA");
    const limiter2 = group1.key("BBB");
    const limiter3 = group1.key("CCC");
    const limiter4 = group1.key("DDD");

    return Promise.all([limiter1.ready(), limiter2.ready(), limiter3.ready(), limiter4.ready()])
      .then(function () {
        expect(group1.connection.id).toEqual("some-id");
        expect(group2.connection.id).toEqual("some-id");
        expect(limiter1.connection.id).toEqual("some-id");
        expect(limiter2.connection.id).toEqual("some-id");
        expect(limiter3.connection.id).toEqual("some-id");
        expect(limiter4.connection.id).toEqual("some-id");
        expect(limiter1.datastore).toEqual(process.env.DATASTORE);
        expect(limiter2.datastore).toEqual(process.env.DATASTORE);
        expect(limiter3.datastore).toEqual(process.env.DATASTORE);
        expect(limiter4.datastore).toEqual(process.env.DATASTORE);

        return Promise.all([
          h.pNoErrVal(limiter1.schedule(h.promise, null, 1), 1),
          h.pNoErrVal(limiter2.schedule(h.promise, null, 2), 2),
          h.pNoErrVal(limiter3.schedule(h.promise, null, 3), 3),
          h.pNoErrVal(limiter4.schedule(h.promise, null, 4), 4),
        ]);
      })
      .then(function () {
        return h.flushLimiter(rootLimiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[1], [2], [3], [4]]);
        return group1.disconnect();
      });
  });

  it("Should not have a key TTL by default for standalone limiters", function () {
    rootLimiter = makeLimiter();

    return rootLimiter
      .ready()
      .then(function () {
        const settings_key = limiterKeys(rootLimiter)[0];
        return runCommand(rootLimiter, "ttl", [settings_key]);
      })
      .then(function (ttl) {
        expect(ttl).toBeLessThan(0);
      });
  });

  it("Should allow timeout setting for standalone limiters", function () {
    rootLimiter = makeLimiter({ timeout: 5 * 60 * 1000 });

    return rootLimiter
      .ready()
      .then(function () {
        const settings_key = limiterKeys(rootLimiter)[0];
        return runCommand(rootLimiter, "ttl", [settings_key]);
      })
      .then(function (ttl) {
        expect(ttl).toBeGreaterThanOrEqual(290);
        expect(ttl).toBeLessThanOrEqual(305);
      });
  });

  it("Should set TTL on all keys including client_* keys after register_client", async function () {
    rootLimiter = makeLimiter({ timeout: 5 * 60 * 1000 });

    await rootLimiter.ready();

    // Get all 8 keys for this limiter
    const keys = limiterKeys(rootLimiter);

    // Identify the client_* keys
    const clientKeys = keys.filter((k) => k.includes("_client_"));

    // First verify that client_* keys actually exist (were created by register_client)
    for (let i = 0; i < clientKeys.length; i++) {
      const key = clientKeys[i];
      const exists = await runCommand(rootLimiter, "exists", [key]);
      assert(exists === 1, `Expected ${key} to exist after register_client, but it doesn't`);
    }

    // Now verify that all keys have TTL set
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const ttl = await runCommand(rootLimiter, "ttl", [key]);

      if (ttl == -2) continue; // key doesn't exist

      // TTL should be around 300 seconds (5 minutes)
      // -1 means no TTL (the bug we're fixing), -2 means key doesn't exist
      assert(
        ttl >= 290 && ttl <= 305,
        `Expected TTL between 290-305 for ${key}, got ${ttl}. ` +
          `(-1 means no TTL set, -2 means key doesn't exist)`,
      );
    }
  });

  it("Should compute reservoir increased based on number of missed intervals", async function () {
    const settings = {
      id: "missed-intervals",
      clearDatastore: false,
      reservoir: 2,
      reservoirIncreaseInterval: 100,
      reservoirIncreaseAmount: 2,
      timeout: 2000,
    };
    rootLimiter = makeLimiter({ ...settings });
    await rootLimiter.ready();

    expect(await rootLimiter.currentReservoir()).toEqual(2);

    const settings_key = limiterKeys(rootLimiter)[0];

    // Boot limiter2 BEFORE shifting lastReservoirIncrease. limiter2's
    // init.lua calls process_tick which runs the catch-up logic, and any
    // wall-clock time spent during limiter2.ready() (a connect-retry can
    // add ~500ms) gets folded into "missed intervals" — which previously
    // shifted the result from the expected 62 to 64+ depending on boot
    // duration. By doing the hset and the read back-to-back as the very
    // last steps, the only Δ between shift-time and read-time is one hset
    // round trip (typically <10ms, well under the 100ms interval).
    const limiter2 = new Bottleneck({ ...settings, datastore: process.env.DATASTORE });
    await limiter2.ready();

    // process_tick uses Date.now() from JS (see RedisDatastore.runScript),
    // not Redis TIME, so we can anchor lastReservoirIncrease to a JS
    // timestamp and the math is deterministic. Reset reservoir to 2
    // because limiter2's init may have already bumped it via process_tick.
    const t = Date.now();
    await runCommand(rootLimiter, "hmset", [
      settings_key,
      "lastReservoirIncrease",
      String(t - 3000),
      "reservoir",
      "2",
    ]);

    // 2 + ((3000 / 100) * 2) === 62 by construction. Allow up to 1 extra
    // missed interval (+2 reservoir) of slop in case the hset round trip
    // crosses the 100ms boundary.
    const reservoir = await rootLimiter.currentReservoir();
    expect(reservoir).toBeGreaterThanOrEqual(62);
    expect(reservoir).toBeLessThanOrEqual(64);

    await limiter2.disconnect();
  });

  it("Should migrate from 2.8.0", function () {
    // Bound the expected timestamps to the test window — not a wall-clock-from-now
    // window that depends on test runtime under load. lastReservoirIncrease is
    // preserved from rootLimiter's init (hsetnx), so the bound must precede that too.
    const testStart = Date.now();
    rootLimiter = makeLimiter({ id: "migrate" });
    const settings_key = limiterKeys(rootLimiter)[0];
    let limiter2;

    return rootLimiter
      .ready()
      .then(function () {
        return Promise.all([
          runCommand(rootLimiter, "hset", [settings_key, "version", "2.8.0"]),
          runCommand(rootLimiter, "hdel", [
            settings_key,
            "done",
            "capacityPriorityCounter",
            "clientTimeout",
          ]),
          runCommand(rootLimiter, "hset", [settings_key, "lastReservoirRefresh", ""]),
        ]);
      })
      .then(function () {
        limiter2 = new Bottleneck({
          id: "migrate",
          datastore: process.env.DATASTORE,
        });
        return limiter2.ready();
      })
      .then(function () {
        return runCommand(rootLimiter, "hmget", [
          settings_key,
          "version",
          "done",
          "reservoirRefreshInterval",
          "reservoirRefreshAmount",
          "capacityPriorityCounter",
          "clientTimeout",
          "reservoirIncreaseAmount",
          "reservoirIncreaseMaximum",
          // Add new values here, before these 2 timestamps
          "lastReservoirRefresh",
          "lastReservoirIncrease",
        ]);
      })
      .then(function (values) {
        const timestamps = values.slice(-2);
        timestamps.forEach(function (t) {
          const num = parseInt(t);
          expect(num).toBeGreaterThanOrEqual(testStart); // timestamp written during this test
          expect(num).toBeLessThanOrEqual(Date.now()); // not somehow in the future
        });
        expect(values.slice(0, -timestamps.length)).toEqual([
          "2.18.0",
          "0",
          "",
          "",
          "0",
          "10000",
          "",
          "",
        ]);
      })
      .then(function () {
        return limiter2.disconnect(false);
      });
  });

  it("Should keep track of each client's queue length", async function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter({
      id: "queues",
      maxConcurrent: 1,
      trackDoneStatus: true,
    });
    const limiter2 = new Bottleneck({
      datastore: process.env.DATASTORE,
      id: "queues",
      maxConcurrent: 1,
      trackDoneStatus: true,
    });
    const client_num_queued_key = limiterKeys(rootLimiter)[5];
    const clientId1 = rootLimiter._store.clientId;
    const clientId2 = limiter2._store.clientId;

    await rootLimiter.ready();
    await limiter2.ready();

    const p0 = rootLimiter.schedule({ id: 0 }, h.slowPromise, 100, null, 0);
    await rootLimiter._submitLock.schedule(() => Promise.resolve());

    const p1 = rootLimiter.schedule({ id: 1 }, h.promise, null, 1);
    const p2 = rootLimiter.schedule({ id: 2 }, h.promise, null, 2);
    const p3 = limiter2.schedule({ id: 3 }, h.promise, null, 3);

    await Promise.all([
      rootLimiter._submitLock.schedule(() => Promise.resolve()),
      limiter2._submitLock.schedule(() => Promise.resolve()),
    ]);

    const queuedA = await runCommand(rootLimiter, "hgetall", [client_num_queued_key]);
    expect(rootLimiter.counts().QUEUED).toEqual(2);
    expect(limiter2.counts().QUEUED).toEqual(1);
    expect(~~queuedA[clientId1]).toEqual(2);
    expect(~~queuedA[clientId2]).toEqual(1);

    expect(await rootLimiter.clusterQueued()).toEqual(3);

    await Promise.all([p0, p1, p2, p3]);
    const queuedB = await runCommand(rootLimiter, "hgetall", [client_num_queued_key]);
    expect(rootLimiter.counts().QUEUED).toEqual(0);
    expect(limiter2.counts().QUEUED).toEqual(0);
    expect(~~queuedB[clientId1]).toEqual(0);
    expect(~~queuedB[clientId2]).toEqual(0);
    expect(rootLimiter.counts().DONE).toEqual(3);
    expect(limiter2.counts().DONE).toEqual(1);

    expect(await rootLimiter.clusterQueued()).toEqual(0);

    return limiter2.disconnect(false);
  });

  it("Should publish capacity increases", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter({ maxConcurrent: 2 });
    let limiter2;

    return rootLimiter
      .ready()
      .then(function () {
        limiter2 = new Bottleneck({ datastore: process.env.DATASTORE });
        return limiter2.ready();
      })
      .then(function () {
        // Use deferredPromise instead of slowPromise(100) for jobs 1 and 2.
        // With slowPromise, the 100ms setTimeout starts at *dispatch* time.
        // Under load, queueing job 0 (and waiting for its dispatch+resolve)
        // can take >100ms — by which point job 1's setTimeout has already
        // fired and pushed [1] before job 0 could push [0]. The expected
        // [[0],[1],[2],[3]] order then flips to [[1],...]. With
        // deferredPromise we hold jobs 1/2 explicitly until job 0 has run,
        // then release them (after a fixed wait that preserves the
        // original ~200ms total duration so capacity-published-to-limiter2
        // semantics are still exercised end-to-end).
        let releaseJobs;
        const jobsSignal = new Promise(function (r) {
          releaseJobs = r;
        });
        rootLimiter.schedule({ id: 1 }, h.deferredPromise, jobsSignal, null, 1);
        rootLimiter.schedule({ id: 2 }, h.deferredPromise, jobsSignal, null, 2);

        return rootLimiter
          .schedule({ id: 0, weight: 0 }, h.promise, null, 0)
          .then(function () {
            return h.wait(100);
          })
          .then(function () {
            releaseJobs();
          });
      })
      .then(function () {
        return limiter2.schedule({ id: 3 }, h.slowPromise, 100, null, 3);
      })
      .then(function () {
        return h.flushLimiter(rootLimiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[0], [1], [2], [3]]);
        // Lower bound ~200ms is the contract: job 0 (instant) + ~100ms
        // hold for jobs 1/2 + ~100ms for job 3 to dispatch on limiter2
        // after capacity opens. We don't tightly upper-bound here — under
        // load, redis round trips + capacity pubsub can stretch this past
        // the original implicit 1200ms cap.
        expect(h.results().elapsed).toBeGreaterThanOrEqual(195);

        return limiter2.disconnect(false);
      });
  });

  it("Should publish capacity changes on reservoir changes", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter({
      maxConcurrent: 2,
      reservoir: 2,
    });
    let limiter2;
    let p3;

    return rootLimiter
      .ready()
      .then(function () {
        limiter2 = new Bottleneck({
          datastore: process.env.DATASTORE,
        });
        return limiter2.ready();
      })
      .then(function () {
        rootLimiter.schedule({ id: 1 }, h.slowPromise, 100, null, 1);
        rootLimiter.schedule({ id: 2 }, h.slowPromise, 100, null, 2);

        return rootLimiter.schedule({ id: 0, weight: 0 }, h.promise, null, 0);
      })
      .then(function () {
        p3 = limiter2.schedule({ id: 3, weight: 2 }, h.slowPromise, 100, null, 3);
        return rootLimiter.currentReservoir();
      })
      .then(function (reservoir) {
        expect(reservoir).toEqual(0);
        return rootLimiter.updateSettings({ reservoir: 1 });
      })
      .then(function () {
        return rootLimiter.incrementReservoir(1);
      })
      .then(function (reservoir) {
        expect(reservoir).toEqual(2);
        return p3;
      })
      .then(function (result) {
        expect(result).toEqual([3]);
        return rootLimiter.currentReservoir();
      })
      .then(function (reservoir) {
        expect(reservoir).toEqual(0);
        return h.flushLimiter(rootLimiter, { weight: 0 });
      })
      .then(function (_results) {
        h.checkResultsOrder([[0], [1], [2], [3]]);
        // Upper bound is generous: a dropped node-redis connection retries
        // in-flight commands after reconnect; with connectTimeout=500ms and
        // one retry cycle the delay can reach ~1500ms on a loaded testcontainer.
        // The ordering assertion above is the real semantic check; checkDuration
        // just guards against a completely silent pub/sub channel (>5s delay).
        h.checkDuration(210, 10, 2000);
      })
      .then(function (_data) {
        return limiter2.disconnect(false);
      });
  });

  it("Should remove track job data and remove lost jobs", function () {
    // Capture before any limiter is constructed; redis-side timestamps may be
    // assigned during rootLimiter's init via hsetnx (see init.lua).
    const testStart = Date.now();
    const h = createJobHarness();
    rootLimiter = makeLimiter({ id: "lost" }, { expectErrors: true });
    const clientId = rootLimiter._store.clientId;
    const limiter1 = new Bottleneck({ datastore: process.env.DATASTORE });
    const limiter2 = new Bottleneck({
      id: "lost",
      datastore: process.env.DATASTORE,
      heartbeatInterval: 150,
    });
    const getData = function (limiter) {
      expect(limiterKeys(limiter).length).toEqual(8); // Asserting, to remember to edit this test when keys change
      const [
        settings_key,
        job_weights_key,
        job_expirations_key,
        job_clients_key,
        client_running_key,
        client_num_queued_key,
        client_last_registered_key,
        client_last_seen_key,
      ] = limiterKeys(limiter);

      return Promise.all([
        runCommand(limiter1, "hmget", [settings_key, "running", "done"]),
        runCommand(limiter1, "hgetall", [job_weights_key]),
        runCommand(limiter1, "zcard", [job_expirations_key]),
        runCommand(limiter1, "hvals", [job_clients_key]),
        runCommand(limiter1, "zrange", [client_running_key, "0", "-1", "withscores"]),
        runCommand(limiter1, "hvals", [client_num_queued_key]),
        runCommand(limiter1, "zrange", [client_last_registered_key, "0", "-1", "withscores"]),
        runCommand(limiter1, "zrange", [client_last_seen_key, "0", "-1", "withscores"]),
      ]);
    };
    let numExpirations = 0;
    const errorHandler = function (err) {
      if (err.message.indexOf("This job timed out") === 0) {
        numExpirations++;
      }
    };

    return (
      Promise.all([rootLimiter.ready(), limiter1.ready(), limiter2.ready()])
        .then(function () {
          // No expiration, it should not be removed
          // oxlint-disable-next-line no-unused-expressions
          (h.pNoErrVal(rootLimiter.schedule({ weight: 1 }, h.slowPromise, 150, null, 1), 1),
            // Expiration present, these jobs should be removed automatically
            rootLimiter
              .schedule({ expiration: 50, weight: 2 }, h.slowPromise, 75, null, 2)
              .catch(errorHandler));
          rootLimiter
            .schedule({ expiration: 50, weight: 3 }, h.slowPromise, 75, null, 3)
            .catch(errorHandler);
          rootLimiter
            .schedule({ expiration: 50, weight: 4 }, h.slowPromise, 75, null, 4)
            .catch(errorHandler);
          rootLimiter
            .schedule({ expiration: 50, weight: 5 }, h.slowPromise, 75, null, 5)
            .catch(errorHandler);

          return rootLimiter._submitLock.schedule(() => Promise.resolve(true));
        })
        .then(function () {
          return rootLimiter._drainAll();
        })
        .then(function () {
          return rootLimiter.disconnect(false);
        })
        // Poll for the post-cleanup state instead of asserting an intermediate
        // snapshot in the narrow window between dispatch and the 50ms expiration
        // timers firing — under event-loop stress that window can effectively
        // vanish, with expirations firing before the snapshot read completes.
        .then(function () {
          return waitForState(async function () {
            const [s, je] = await Promise.all([
              runCommand(limiter1, "hmget", [limiterKeys(rootLimiter)[0], "running", "done"]),
              runCommand(limiter1, "zcard", [limiterKeys(rootLimiter)[2]]),
            ]);
            expect(s[0]).toBe("1");
            expect(s[1]).toBe("14");
            expect(je).toBe(0);
            expect(numExpirations).toBe(4);
          });
        })
        .then(function () {
          return getData(rootLimiter);
        })
        .then(function ([
          settings,
          job_weights,
          job_expirations,
          job_clients,
          client_running,
          client_num_queued,
          client_last_registered,
          client_last_seen,
        ]) {
          expect(settings).toEqual(["1", "14"]);
          expect(sumWeights(job_weights)).toEqual(1);
          expect(job_expirations).toEqual(0);
          expect(job_clients.length).toEqual(1);
          job_clients.forEach((id) => expect(id).toEqual(clientId));
          expect(sumWeights(client_running)).toEqual(1);
          expect(client_num_queued).toEqual(["0", "0"]);
          expect(client_last_registered[1]).toEqual("0");
          expect(parseFloat(client_last_seen[1])).toBeGreaterThanOrEqual(testStart);
          expect(parseFloat(client_last_seen[1])).toBeLessThanOrEqual(Date.now());
          // Limiter2's registration timestamp falls within the test window.
          expect(parseFloat(client_last_registered[3])).toBeGreaterThanOrEqual(testStart);
          expect(parseFloat(client_last_registered[3])).toBeLessThanOrEqual(Date.now());

          expect(numExpirations).toEqual(4);
        })
        .then(function () {
          return Promise.all([limiter1.disconnect(false), limiter2.disconnect(false)]);
        })
    );
  });

  it("Should clear unresponsive clients", async function () {
    rootLimiter = makeLimiter({
      id: "unresponsive",
      maxConcurrent: 1,
      timeout: 1000,
      // 500ms gives 10x margin over a typical cleanup cycle while still
      // being well within the 5s waitFor window. The original 100ms was
      // too tight for a loaded shared testcontainer and caused rare (~3%)
      // timeouts when Redis command round-trips briefly delayed the
      // process_tick clock.
      clientTimeout: 500,
      heartbeatInterval: 50,
    });
    // rootLimiter must finish init.lua first so shared settings adopt its
    // clientTimeout/heartbeatInterval. If limiter2 wins the race with
    // default options, clientTimeout=10000 and process_tick can't clean
    // up within the 5s test window.
    await rootLimiter.ready();
    const limiter2 = new Bottleneck({
      id: "unresponsive",
      datastore: process.env.DATASTORE,
    });
    await limiter2.ready();
    await Promise.all([rootLimiter.running(), limiter2.running()]);

    const client_running_key = limiterKeys(limiter2)[4];
    const client_num_queued_key = limiterKeys(limiter2)[5];
    const client_last_registered_key = limiterKeys(limiter2)[6];
    const client_last_seen_key = limiterKeys(limiter2)[7];
    const numClients = () =>
      Promise.all([
        runCommand(rootLimiter, "zcard", [client_running_key]),
        runCommand(rootLimiter, "hlen", [client_num_queued_key]),
        runCommand(rootLimiter, "zcard", [client_last_registered_key]),
        runCommand(rootLimiter, "zcard", [client_last_seen_key]),
      ]);

    expect(await numClients()).toEqual([2, 2, 2, 2]);

    await limiter2.disconnect(false);

    // Poll for cleanup. Cleanup happens in process_tick.lua, triggered by
    // limiter operations. Each poll calls running() which fires process_tick.
    await waitForState(
      async function () {
        await rootLimiter.running();
        const counts = await numClients();
        expect(counts[0]).toBe(1);
        expect(counts[1]).toBe(1);
        expect(counts[2]).toBe(1);
        expect(counts[3]).toBe(1);
      },
      { timeout: 5000 },
    );

    expect(await numClients()).toEqual([1, 1, 1, 1]);
  });

  it("Should not clear unresponsive clients with unexpired running jobs", async function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter({
      id: "unresponsive-unexpired",
      maxConcurrent: 1,
      timeout: 1000,
      clientTimeout: 200,
      heartbeatInterval: 2000,
    });
    // Sequence init so rootLimiter's clientTimeout/heartbeatInterval win over
    // limiter2's defaults. Constructing limiter2 up-front races init.lua and
    // settings can adopt the wrong values.
    await rootLimiter.ready();
    const limiter2 = new Bottleneck({
      id: "unresponsive-unexpired",
      datastore: process.env.DATASTORE,
    });
    await limiter2.ready();

    const client_running_key = limiterKeys(limiter2)[4];
    const client_num_queued_key = limiterKeys(limiter2)[5];
    const client_last_registered_key = limiterKeys(limiter2)[6];
    const client_last_seen_key = limiterKeys(limiter2)[7];
    const numClients = () =>
      Promise.all([
        runCommand(limiter2, "zcard", [client_running_key]),
        runCommand(limiter2, "hlen", [client_num_queued_key]),
        runCommand(limiter2, "zcard", [client_last_registered_key]),
        runCommand(limiter2, "zcard", [client_last_seen_key]),
      ]);

    const job = rootLimiter.schedule(h.slowPromise, 500, null, 1);

    await h.wait(300);

    // running() triggers process_tick and that will attempt to remove client 1
    // but it shouldn't do it because it has a running job
    expect(await limiter2.running()).toEqual(1);

    expect(await numClients()).toEqual([2, 2, 2, 2]);

    await job;

    expect(await limiter2.running()).toEqual(0);

    await limiter2.disconnect(false);
  });

  it("Should clear unresponsive clients after last jobs are expired", async function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter({
      id: "unresponsive-expired",
      maxConcurrent: 1,
      timeout: 1000,
      clientTimeout: 200,
      heartbeatInterval: 2000,
    });
    // Sequence init so rootLimiter's clientTimeout/heartbeatInterval win over
    // limiter2's defaults.
    await rootLimiter.ready();
    const limiter2 = new Bottleneck({
      id: "unresponsive-expired",
      datastore: process.env.DATASTORE,
    });
    await limiter2.ready();

    const client_running_key = limiterKeys(limiter2)[4];
    const client_num_queued_key = limiterKeys(limiter2)[5];
    const client_last_registered_key = limiterKeys(limiter2)[6];
    const client_last_seen_key = limiterKeys(limiter2)[7];
    const numClients = () =>
      Promise.all([
        runCommand(limiter2, "zcard", [client_running_key]),
        runCommand(limiter2, "hlen", [client_num_queued_key]),
        runCommand(limiter2, "zcard", [client_last_registered_key]),
        runCommand(limiter2, "zcard", [client_last_seen_key]),
      ]);

    const job = rootLimiter.schedule({ expiration: 250 }, h.slowPromise, 300, null, 1);
    await h.wait(100); // wait for it to register

    expect(await rootLimiter.running()).toEqual(1);
    expect(await numClients()).toEqual([2, 2, 2, 2]);

    let dropped = false;
    try {
      await job;
    } catch (e) {
      if (e.message === "This job timed out after 250 ms.") {
        dropped = true;
      } else {
        throw e;
      }
    }
    assert(dropped, "Expected dropped to be true");

    // Cleanup happens in process_tick.lua, triggered by limiter operations.
    // Poll instead of relying on a fixed wait — under load the cleanup might
    // need more than 200ms wall-clock, and a fixed wait either fails (too short)
    // or wastes time (too long). Each poll calls running() which fires process_tick.
    await waitForState(
      async function () {
        await limiter2.running();
        const counts = await numClients();
        expect(counts[0]).toBe(1);
        expect(counts[1]).toBe(1);
        expect(counts[2]).toBe(1);
        expect(counts[3]).toBe(1);
      },
      { timeout: 5000 },
    );

    expect(await limiter2.running()).toEqual(0);
    expect(await numClients()).toEqual([1, 1, 1, 1]);

    await limiter2.disconnect(false);
  });

  it("Should use shared settings", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter({ maxConcurrent: 2 });
    let limiter2;
    const settings_key = limiterKeys(rootLimiter)[0];

    // rootLimiter must finish init.lua first so it owns the initial settings;
    // limiter2 then attaches without `clearDatastore`, so its constructor
    // values must be ignored in favor of the shared settings. Constructing
    // both up-front and using Promise.all races init.lua executions and
    // produces flaky reads.
    return rootLimiter
      .ready()
      .then(function () {
        limiter2 = new Bottleneck({ maxConcurrent: 1, datastore: process.env.DATASTORE });
        return limiter2.ready();
      })
      .then(function () {
        return runCommand(rootLimiter, "hget", [settings_key, "maxConcurrent"]);
      })
      .then(function (maxConcurrent) {
        expect(maxConcurrent).toEqual("2");
        return Promise.all([
          limiter2.schedule(h.slowPromise, 100, null, 1),
          limiter2.schedule(h.slowPromise, 100, null, 2),
        ]);
      })
      .then(function () {
        return limiter2.disconnect(false);
      })
      .then(function () {
        return h.flushLimiter(rootLimiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[1], [2]]);
      });
  });

  it("Should clear previous settings", function () {
    const h = createJobHarness();
    rootLimiter = makeLimiter({ maxConcurrent: 2 });
    let limiter2;
    const settings_key = limiterKeys(rootLimiter)[0];

    return rootLimiter
      .ready()
      .then(function () {
        limiter2 = new Bottleneck({
          maxConcurrent: 1,
          datastore: process.env.DATASTORE,
          clearDatastore: true,
        });
        return limiter2.ready();
      })
      .then(function () {
        // Verify the actual cleared setting in redis directly — this is the
        // contract being tested. Avoids dependence on slowPromise wall-clock
        // timing which can slip under load (event-loop delay, GC, redis stalls).
        return runCommand(rootLimiter, "hget", [settings_key, "maxConcurrent"]);
      })
      .then(function (maxConcurrent) {
        expect(maxConcurrent).toEqual("1");
        return Promise.all([
          rootLimiter.schedule(h.slowPromise, 100, null, 1),
          rootLimiter.schedule(h.slowPromise, 100, null, 2),
        ]);
      })
      .then(function () {
        return limiter2.disconnect(false);
      })
      .then(function () {
        return h.flushLimiter(rootLimiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[1], [2]]);
      });
  });

  it("Should safely handle connection failures", function () {
    expect.hasAssertions();
    // node-redis v4+ uses a nested socket option shape; ioredis stays flat.
    const failingOptions =
      process.env.DATASTORE === "redis"
        ? { socket: { port: 1, reconnectStrategy: () => false } }
        : { port: 1 };
    rootLimiter = makeLimiter({ clientOptions: failingOptions }, { expectErrors: true });

    return new Promise(function (resolve, reject) {
      rootLimiter.on("error", function (err) {
        expect(err).toBeTruthy();
        resolve();
      });

      rootLimiter.ready().then(
        function () {
          reject(new Error("Should not have connected"));
        },
        function () {
          /* node-redis/ioredis may reject ready(); the limiter "error" event is authoritative */
        },
      );
    });
  });
});
