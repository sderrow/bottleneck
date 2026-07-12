import { describe, expect } from "vitest";
import sleep from "../src/sleep.js";
import { test, waitForState, deferred, enqueued } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");
const Scripts = require("../src/cluster/Scripts.js");

// Causality policy (Workstream B): observe product-timer effects via waitForState
// and state counts — never assert wall-clock bounds around real network time.

const limiterKeys = (limiter) => Scripts.allKeys(limiter._store.originalId);
const countKeys = (limiter) => runCommand(limiter, "exists", limiterKeys(limiter));
const deleteKeys = (limiter) => runCommand(limiter, "del", limiterKeys(limiter));
const runCommand = (limiter, command, args) =>
  limiter._store.connection.__runCommand__([command, ...args]);
const runningOrExecuting = (limiter) => {
  const counts = limiter.counts();
  return counts.RUNNING + counts.EXECUTING;
};
describe("Cluster coordination", () => {
  if (process.env.DATASTORE !== "redis" && process.env.DATASTORE !== "ioredis") {
    throw new Error("DATASTORE must be redis or ioredis");
  }

  test("Should chain local and distributed limiters (total concurrency)", async ({
    harness: h,
    makeLimiter,
    track,
  }) => {
    const rootLimiter = makeLimiter({ id: "limiter1", maxConcurrent: 3 });
    const limiter2 = track(new Bottleneck({ id: "limiter2", maxConcurrent: 1 }));
    const limiter3 = track(new Bottleneck({ id: "limiter3", maxConcurrent: 2 }));

    limiter2.on("error", (err) => console.log(err));
    limiter2.chain(rootLimiter);
    limiter3.chain(rootLimiter);

    await Promise.all([rootLimiter.ready(), limiter2.ready(), limiter3.ready()]);

    const sig1 = deferred();
    const sig2 = deferred();
    const sig3 = deferred();
    const sig4 = deferred();
    const sig5 = deferred();
    const sig6 = deferred();

    const p1 = limiter2.schedule(h.deferredPromise, sig1.signal, null, 1);
    const p2 = limiter2.schedule(h.deferredPromise, sig2.signal, null, 2);
    const p3 = limiter2.schedule(h.deferredPromise, sig3.signal, null, 3);
    const p4 = limiter3.schedule(h.deferredPromise, sig4.signal, null, 4);
    const p5 = limiter3.schedule(h.deferredPromise, sig5.signal, null, 5);
    const p6 = limiter3.schedule(h.deferredPromise, sig6.signal, null, 6);

    await waitForState(() => {
      expect(rootLimiter.counts().EXECUTING).toBe(3);
      expect(limiter2.counts().QUEUED).toBe(2);
      expect(limiter3.counts().QUEUED).toBe(1);
    });

    sig1.release();
    sig4.release();
    sig5.release();
    await waitForState(() => {
      expect(h.log.mock.calls.length).toBe(3);
    });

    sig2.release();
    sig6.release();
    await waitForState(() => {
      expect(h.log.mock.calls.length).toBe(5);
    });

    sig3.release();
    await Promise.all([p1, p2, p3, p4, p5, p6]);
    await h.flushLimiter(rootLimiter);
    expect(h.log).toHaveCallOrder([[1], [4], [5], [2], [6], [3]]);
  });

  test("Should chain local and distributed limiters (partial concurrency)", async ({
    harness: h,
    makeLimiter,
    track,
  }) => {
    const rootLimiter = makeLimiter({ maxConcurrent: 2 });
    const limiter2 = track(new Bottleneck({ maxConcurrent: 1 }));
    const limiter3 = track(new Bottleneck({ maxConcurrent: 2 }));

    limiter2.chain(rootLimiter);
    limiter3.chain(rootLimiter);

    await Promise.all([rootLimiter.ready(), limiter2.ready(), limiter3.ready()]);

    const sig1 = deferred();
    const sig2 = deferred();
    const sig3 = deferred();
    const sig4 = deferred();
    const sig5 = deferred();
    const sig6 = deferred();

    const p1 = limiter2.schedule(h.deferredPromise, sig1.signal, null, 1);
    const p2 = limiter2.schedule(h.deferredPromise, sig2.signal, null, 2);
    const p3 = limiter2.schedule(h.deferredPromise, sig3.signal, null, 3);
    const p4 = limiter3.schedule(h.deferredPromise, sig4.signal, null, 4);
    const p5 = limiter3.schedule(h.deferredPromise, sig5.signal, null, 5);
    const p6 = limiter3.schedule(h.deferredPromise, sig6.signal, null, 6);

    await waitForState(() => {
      expect(rootLimiter.counts().EXECUTING).toBe(2);
      expect(limiter2.counts().QUEUED).toBe(2);
      expect(limiter3.counts().QUEUED).toBe(1);
    });

    sig1.release();
    sig4.release();
    sig5.release();
    await waitForState(() => {
      expect(h.log.mock.calls.length).toBe(3);
    });

    sig2.release();
    sig6.release();
    await waitForState(() => {
      expect(h.log.mock.calls.length).toBe(5);
    });

    sig3.release();
    await Promise.all([p1, p2, p3, p4, p5, p6]);
    await h.flushLimiter(rootLimiter);
    expect(h.log).toHaveCallOrder([[1], [4], [5], [2], [6], [3]]);
  });

  test("Should use the limiter ID to build Redis keys", async ({ makeLimiter, track }) => {
    const rootLimiter = makeLimiter();
    const randomId = rootLimiter._randomIndex();
    const limiter = track(
      new Bottleneck({
        id: randomId,
        datastore: process.env.DATASTORE,
        clearDatastore: true,
      }),
    );

    await limiter.ready();
    const keys = limiterKeys(limiter);
    keys.forEach((key) => expect(key.indexOf(randomId)).toBeGreaterThan(0));
    const deleted = await deleteKeys(limiter);
    expect(deleted).toEqual(5);
  });

  test("Should not fail when Redis data is missing", async ({ track }) => {
    const limiter = track(
      new Bottleneck({ datastore: process.env.DATASTORE, clearDatastore: true }),
    );

    const runningBefore = await limiter.running();
    expect(runningBefore).toEqual(0);
    const deleted = await deleteKeys(limiter);
    expect(deleted).toEqual(5);
    const countAfterDelete = await countKeys(limiter);
    expect(countAfterDelete).toEqual(0);
    const runningAfter = await limiter.running();
    expect(runningAfter).toEqual(0);
    const countRecreated = await countKeys(limiter);
    expect(countRecreated).toBeGreaterThan(0);
  });

  test("Should drop all jobs in the Cluster when entering blocked mode", async ({
    harness: h,
    makeLimiter,
    track,
  }) => {
    const rootLimiter = makeLimiter();
    const limiter1 = track(
      new Bottleneck({
        id: "blocked",
        trackDoneStatus: true,
        datastore: process.env.DATASTORE,
        clearDatastore: true,

        maxConcurrent: 1,
        minTime: 50,
        highWater: 2,
        strategy: Bottleneck.strategy.BLOCK,
      }),
    );
    const client_num_queued_key = limiterKeys(limiter1)[5];

    await limiter1.ready();
    const limiter2 = track(
      new Bottleneck({
        id: "blocked",
        trackDoneStatus: true,
        datastore: process.env.DATASTORE,
        clearDatastore: false,
      }),
    );
    await limiter2.ready();

    // Fire jobs 1-2 on limiter1, then wait for enqueued(limiter1) so both
    // registrations reach redis before limiter2's jobs — preserving the
    // enqueue order the old awaited submit groups pinned. Job 2 is NOT
    // dropped yet at this point: blocked mode only trips once limiter2's
    // submissions push the cluster queue to highWater, so p2's rejection
    // cannot be awaited before jobs 3-5 are fired.
    const p1 = limiter1.schedule(h.slowPromise, 100, null, 1);
    const p2 = limiter1.schedule(h.slowPromise, 100, null, 2);
    await enqueued(limiter1);
    // Jobs 3-5 trip blocked mode, which drops jobs 2-5 cluster-wide and
    // rejects their schedule promises (default rejectOnDrop). The .rejects
    // assertions join the barrier's Promise.all: they attach synchronously
    // with these schedules — before the drops fire during the registration
    // round-trips — so vitest never sees an unhandled rejection, and the
    // await point covers all four drops, which the old test confirmed via
    // the queue counts right below.
    const p3 = limiter2.schedule(h.slowPromise, 100, null, 3);
    const p4 = limiter2.schedule(h.slowPromise, 100, null, 4);
    const p5 = limiter2.schedule(h.slowPromise, 100, null, 5);
    await Promise.all([
      enqueued(limiter2),
      expect(p2).rejects.toThrow("This job has been dropped by Bottleneck"),
      expect(p3).rejects.toThrow("This job has been dropped by Bottleneck"),
      expect(p4).rejects.toThrow("This job has been dropped by Bottleneck"),
      expect(p5).rejects.toThrow("This job has been dropped by Bottleneck"),
    ]);

    const queues = await runCommand(limiter1, "hvals", [client_num_queued_key]);
    expect(queues).toEqual(["0", "0"]);

    const clusterQueues = await Promise.all([
      rootLimiter.clusterQueued(),
      limiter2.clusterQueued(),
    ]);
    expect(clusterQueues).toEqual([0, 0]);

    // Job 1 is the only survivor; its completion is confirmed alongside the
    // drop counts the old test checked here.
    await expect(p1).resolves.toEqual([1]);

    // Poll for the final settled state instead of a fixed wait. Under
    // event-loop stress (sustained test runs), setTimeout(100) can slip
    // multiple seconds and break a wall-clock-based wait. We give
    // 5000ms (default 2000ms is not always enough): j1's 100ms
    // slowPromise can dispatch hundreds of ms late under load (a single
    // connect-retry on the ioredis client adds ~500ms to register;
    // doExecute's setTimeout(0) drifts when the event loop is busy
    // serving other parallel test workers).
    await waitForState(() => {
      const c1 = limiter1.counts();
      expect(c1.RECEIVED).toBe(0);
      expect(c1.QUEUED).toBe(0);
      expect(c1.RUNNING).toBe(0);
      expect(c1.EXECUTING).toBe(0);
      expect(c1.DONE).toBe(1);
    });

    const counts1 = limiter1.counts();
    expect(counts1.RECEIVED).toEqual(0);
    expect(counts1.QUEUED).toEqual(0);
    expect(counts1.RUNNING).toEqual(0);
    expect(counts1.EXECUTING).toEqual(0);
    expect(counts1.DONE).toEqual(1);

    const counts2 = limiter2.counts();
    expect(counts2.RECEIVED).toEqual(0);
    expect(counts2.QUEUED).toEqual(0);
    expect(counts2.RUNNING).toEqual(0);
    expect(counts2.EXECUTING).toEqual(0);
    expect(counts2.DONE).toEqual(0);

    await h.flushLimiter(rootLimiter);
    expect(h.log).toHaveCallOrder([[1]]);
  });

  test("Should pass messages to all limiters in Cluster", async ({ makeLimiter, track }) => {
    const rootLimiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      id: "super-duper",
    });
    const limiter1 = track(
      new Bottleneck({
        maxConcurrent: 1,
        minTime: 100,
        id: "super-duper",
        datastore: process.env.DATASTORE,
      }),
    );
    const limiter2 = track(
      new Bottleneck({
        maxConcurrent: 1,
        minTime: 100,
        id: "nope",
        datastore: process.env.DATASTORE,
      }),
    );
    const received = [];

    rootLimiter.on("message", (msg) => {
      received.push(1, msg);
    });
    limiter1.on("message", (msg) => {
      received.push(2, msg);
    });
    limiter2.on("message", (msg) => {
      received.push(3, msg);
    });

    await Promise.all([rootLimiter.ready(), limiter1.ready(), limiter2.ready()]);

    limiter1.publish(555);
    await waitForState(() => {
      expect(received.length).toBeGreaterThanOrEqual(4);
    });

    limiter1.disconnect();
    limiter2.disconnect();
    expect(received.sort()).toEqual([1, 2, "555", "555"]);
  });

  test("Should pass messages to correct limiter after Group re-instantiations", async ({
    track,
  }) => {
    const group = track(
      new Bottleneck.Group({
        maxConcurrent: 1,
        minTime: 100,
        datastore: process.env.DATASTORE,
      }),
    );
    const received = [];

    await new Promise((resolve, _reject) => {
      const limiter = group.key("A");

      limiter.on("message", (msg) => {
        received.push("1", msg);
        return resolve();
      });
      limiter.publish("Bonjour!");
    });

    await new Promise((resolve, _reject) => {
      const limiter = group.key("B");

      limiter.on("message", (msg) => {
        received.push("2", msg);
        return resolve();
      });
      limiter.publish("Comment allez-vous?");
    });

    await group.deleteKey("A");

    await new Promise((resolve, _reject) => {
      const limiter = group.key("A");

      limiter.on("message", (msg) => {
        received.push("3", msg);
        return resolve();
      });
      limiter.publish("Au revoir!");
    });

    expect(received).toEqual(["1", "Bonjour!", "2", "Comment allez-vous?", "3", "Au revoir!"]);
    // Semantic, not cleanup: flush=true gracefully drains the un-awaited
    // "Au revoir!" PUBLISH reply before closing. track's disconnect(false)
    // would destroy the socket mid-flight and reject that pending command.
    group.disconnect();
  });

  test("Should have a default key TTL when using Groups", async ({ track }) => {
    const group = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
      }),
    );

    await group.key("one").ready();
    const limiter = group.key("one");
    const settings_key = limiterKeys(limiter)[0];
    const ttl = await runCommand(limiter, "ttl", [settings_key]);
    expect(ttl).toBeGreaterThanOrEqual(290);
    expect(ttl).toBeLessThanOrEqual(305);
  });

  test("Should support Groups and expire Redis keys", async ({ makeLimiter, track }) => {
    const rootLimiter = makeLimiter();
    const group = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        minTime: 50,
        timeout: 200,
      }),
    );

    const t0 = Date.now();
    const results = {};
    const job = (x) => {
      results[x] = Date.now() - t0;
      return Promise.resolve();
    };

    await rootLimiter.ready();
    const limiter1 = group.key("one");
    const limiter2 = group.key("two");
    const limiter3 = group.key("three");

    await Promise.all([limiter1.ready(), limiter2.ready(), limiter3.ready()]);

    const counts = await Promise.all([
      countKeys(limiter1),
      countKeys(limiter2),
      countKeys(limiter3),
    ]);
    expect(counts).toEqual([5, 5, 5]);

    await Promise.all([
      limiter1.schedule(job, "a"),
      limiter1.schedule(job, "b"),
      limiter1.schedule(job, "c"),
      limiter2.schedule(job, "d"),
      limiter2.schedule(job, "e"),
      limiter3.schedule(job, "f"),
    ]);

    // The CONTRACT this test verifies is "Bottleneck.Group creates
    // independent per-key limiters that can run in parallel, and Redis
    // keys are eventually cleaned up". We verify dispatch ORDER (which
    // is guaranteed by FIFO + minTime gating inside Bottleneck) and
    // job count, but DELIBERATELY do not assert per-pair gap timings
    // here. Reason: results[x] = Date.now() - t0 is recorded inside
    // the job body, and under heavy event-loop load (testcontainer
    // boot, parallel test files, etc.) several pending bodies can
    // run in the same event-loop tick — collapsing Date.now() to the
    // same millisecond. minTime spacing is still upheld inside the
    // limiter, just not externally measurable from here. Stronger
    // minTime assertions live in the simpler priority/general-traffic
    // tests where we control the event loop directly.
    expect(Object.keys(results).length).toEqual(6);
    expect(results.a).toBeLessThanOrEqual(results.b);
    expect(results.b).toBeLessThanOrEqual(results.c);
    expect(results.d).toBeLessThanOrEqual(results.e);

    // Different limiters in the same group should dispatch in parallel
    // (no shared minTime/maxConcurrent). Tolerate dispatch jitter —
    // simultaneous dispatches across separate limiters drift slightly
    // under load even though the intended behavior is "fire together".
    expect(Math.abs(results.a - results.d)).toBeLessThanOrEqual(100);
    expect(Math.abs(results.d - results.f)).toBeLessThanOrEqual(100);
    expect(Math.abs(results.b - results.e)).toBeLessThanOrEqual(100);

    // Poll for autocleanup AND the underlying disconnect to settle.
    // group.deleteKey removes from instances synchronously but awaits
    // instance.disconnect() which clears connection.limiters async — both
    // need to be flushed before the assertions below.
    await waitForState(() => {
      expect(group.keys().length).toBe(0);
      expect(Object.keys(group.connection.limiters).length).toBe(0);
    });

    const countsAfterCleanup = await Promise.all([
      countKeys(limiter1),
      countKeys(limiter2),
      countKeys(limiter3),
    ]);
    expect(countsAfterCleanup).toEqual([0, 0, 0]);
    expect(group.keys().length).toEqual(0);
    expect(Object.keys(group.connection.limiters).length).toEqual(0);
  });

  test("Should not recreate a key when running heartbeat", async ({ harness: h, track }) => {
    const group = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        maxConcurrent: 50,
        minTime: 50,
        timeout: 300,
        heartbeatInterval: 5,
      }),
    );
    const key = "heartbeat";

    const limiter = group.key(key);
    await expect(limiter.schedule(h.promise, null, 1)).resolves.toEqual([1]);
    const doneCount = await limiter.done();
    expect(doneCount).toEqual(1);
    await sleep(400);
    const count = await countKeys(limiter);
    expect(count).toEqual(0);
  });

  test("Should delete Redis key when manually deleting a group key", async ({
    harness: h,
    track,
  }) => {
    // Bump timeout (and the corresponding waitForState below) so autocleanup
    // doesn't race with the initial schedule under stress. Original 300ms
    // gave a 150ms autocleanup interval that could fire before init.lua
    // settled when redis was slow.
    const groupTimeout = 5000;
    const group1 = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        maxConcurrent: 50,
        minTime: 50,
        timeout: groupTimeout,
      }),
    );
    const group2 = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        maxConcurrent: 50,
        minTime: 50,
        timeout: groupTimeout,
      }),
    );
    const key = "deleted";
    const limiter = group1.key(key); // only for countKeys() use

    await expect(group1.key(key).schedule(h.promise, null, 1)).resolves.toEqual([1]);
    await expect(group2.key(key).schedule(h.promise, null, 2)).resolves.toEqual([2]);

    expect(group1.keys().length).toEqual(1);
    expect(group2.keys().length).toEqual(1);
    await group1.key(key).running();

    // Call deleteKey ONCE and assert its return value — retrying a delete
    // until it returns true would mask a regression where the first call
    // wrongly returns false. group1 holds the local instance, so true is
    // guaranteed structurally (instance != null short-circuits).
    const deleted = await group1.deleteKey(key);
    expect(deleted).toEqual(true);

    const count = await countKeys(limiter);
    expect(count).toEqual(0);
    expect(group1.keys().length).toEqual(0);
    expect(group2.keys().length).toEqual(1);
    // Poll for group2's autocleanup to detect the missing redis key and
    // prune the local instance — fires every groupTimeout/2 ms.
    await waitForState(
      () => {
        expect(group2.keys().length).toBe(0);
      },
      { timeout: groupTimeout * 2 },
    );

    expect(group1.keys().length).toEqual(0);
    expect(group2.keys().length).toEqual(0);
  });

  test("Should delete Redis keys from a group even when the local limiter is not present", async ({
    harness: h,
    track,
  }) => {
    // groupTimeout pulls double duty here: it sets the redis-side TTL
    // (must not expire before group2.deleteKey runs), and it gates
    // autocleanup interval (timeout/2). 2000ms was enough for autocleanup
    // to fire within the waitFor window, but tight enough that under stress
    // the keys could TTL-expire before deleteKey ran. Refreshing the TTL
    // explicitly via running() right before deleteKey decouples the two.
    const groupTimeout = 5000;
    const group1 = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        maxConcurrent: 50,
        minTime: 50,
        timeout: groupTimeout,
      }),
    );
    const group2 = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        maxConcurrent: 50,
        minTime: 50,
        timeout: groupTimeout,
      }),
    );
    const key = "deleted-cluster-wide";
    const limiter = group1.key(key); // only for countKeys() use

    await expect(group1.key(key).schedule(h.promise, null, 1)).resolves.toEqual([1]);

    expect(group1.keys().length).toEqual(1);
    expect(group2.keys().length).toEqual(0);
    await group1.key(key).running();

    // The keys were written through group1's connection; poll read-only
    // existence before the cross-group delete so a slow write can't turn
    // this into a false failure...
    await waitForState(async () => {
      expect(await countKeys(limiter)).toBeGreaterThan(0);
    });

    // ...then call deleteKey ONCE and assert its return value. group2 has
    // no local instance, so the value reflects the redis DEL — retrying
    // until true would mask a regression where it wrongly returns false.
    const deleted = await group2.deleteKey(key);
    expect(deleted).toEqual(true);

    const count = await countKeys(limiter);
    expect(count).toEqual(0);
    expect(group1.keys().length).toEqual(1);
    expect(group2.keys().length).toEqual(0);
    // Poll for group1's autocleanup to detect the missing redis key.
    await waitForState(
      () => {
        expect(group1.keys().length).toBe(0);
      },
      { timeout: groupTimeout * 2 },
    );

    expect(group1.keys().length).toEqual(0);
    expect(group2.keys().length).toEqual(0);
  });

  test("Should returns all Group keys in the cluster", async ({ track }) => {
    // Use a long timeout so redis-side TTLs cannot expire mid-test under load.
    // Original 3000ms was tight enough that a slow run (cumulative redis latency)
    // could let keys expire before the assertions, then autocleanup would prune
    // them from instances and group.keys() would surprisingly return [].
    const group1 = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "same",
        timeout: 30000,
      }),
    );
    const group2 = track(
      new Bottleneck.Group({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "same",
        timeout: 30000,
      }),
    );
    const keys1 = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur"];
    const keys2 = ["adipiscing", "elit"];
    const both = keys1.concat(keys2);

    await Promise.all(keys1.map((k) => group1.key(k).ready()));
    await Promise.all(keys2.map((k) => group2.key(k).ready()));

    expect(group1.keys().sort()).toEqual(keys1.sort());
    expect(group2.keys().sort()).toEqual(keys2.sort());
    expect((await group1.clusterKeys()).sort()).toEqual(both.sort());
    expect((await group1.clusterKeys()).sort()).toEqual(both.sort());

    const group3 = track(new Bottleneck.Group({ datastore: "local" }));
    expect(await group3.clusterKeys()).toEqual([]);
  });

  test("Should queue up the least busy limiter", async ({ harness: h, track }) => {
    const limiter1 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );
    const limiter2 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );
    const limiter3 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );
    const limiter4 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );

    await limiter1.schedule({ id: "1" }, h.promise, null, "A");
    await limiter2.schedule({ id: "2" }, h.promise, null, "B");
    await limiter3.schedule({ id: "3" }, h.promise, null, "C");
    await limiter4.schedule({ id: "4" }, h.promise, null, "D");

    // Hold A open with a deferred job so cluster capacity stays at 3 while
    // D/E/F/G queue, regardless of how long the registration round-trips take.
    // We release A after the QUEUED assertions so it finishes before B,
    // preserving the original [1, 4, 5, 6, 7, 2, 3] completion order.
    //
    // Registration order is load-bearing (client_last_registered feeds the
    // capacity-priority grants), so each schedule is followed by a drain of
    // that limiter's enqueued() barrier — the enqueue-time guarantee the old
    // sequentially-awaited submits provided.
    const sigA = deferred();

    const p1 = limiter1.schedule({ id: "A" }, h.deferredPromise, sigA.signal, null, 1);
    await enqueued(limiter1);
    // B and C must finish after D/E/F/G. Use generous durations so the test
    // is robust to redis round-trip delays between releaseA() and G's completion.
    const p2 = limiter1.schedule({ id: "B" }, h.slowPromise, 1000, null, 2);
    await enqueued(limiter1);
    const p3 = limiter2.schedule({ id: "C" }, h.slowPromise, 1050, null, 3);
    await enqueued(limiter2);

    expect(runningOrExecuting(limiter1)).toEqual(2);
    expect(runningOrExecuting(limiter2)).toEqual(1);

    const p4 = limiter3.schedule({ id: "D" }, h.slowPromise, 50, null, 4);
    await enqueued(limiter3);
    const p5 = limiter4.schedule({ id: "E" }, h.slowPromise, 50, null, 5);
    await enqueued(limiter4);
    const p6 = limiter3.schedule({ id: "F" }, h.slowPromise, 50, null, 6);
    await enqueued(limiter3);
    const p7 = limiter4.schedule({ id: "G" }, h.slowPromise, 50, null, 7);
    await enqueued(limiter4);

    expect(limiter3.counts().QUEUED).toEqual(2);
    expect(limiter4.counts().QUEUED).toEqual(2);

    sigA.release();

    await Promise.all([p1, p2, p3, p4, p5, p6, p7]);

    // The CONTRACT here is "Bottleneck distributes cluster capacity to the
    // least-busy limiter" — i.e. D/E spread to limiter3/limiter4 (instead of
    // both stacking on one), then F/G, while limiter1/limiter2 finish their
    // long slowJobs last. We verify:
    //   - The four warm-up "promise" jobs run first in [A,B,C,D] order
    //     (sequentially awaited, instant).
    //   - Job 1 (A's deferred slot) runs next, immediately after releaseA().
    //   - Jobs {4,5} (the FIRST queued job on each of limiter3+limiter4) run
    //     next — distribution proof.
    //   - Jobs {6,7} (the SECOND queued job on each) run after that —
    //     intra-limiter FIFO + continued distribution.
    //   - Jobs 2,3 (1000/1050ms slowJobs on limiter1/limiter2) finish last.
    // We deliberately do NOT pin the F-vs-G order (i.e. {6,7}): when both
    // limiters have 0 running and 1 queued, the "least busy" tiebreaker is
    // resolved via a Redis-side capacity grant whose order is sensitive to
    // pubsub round-trip latency under load. Strict ordering here was the
    // observed flake (e.g. [..,4,5,7,6,..]).
    const calls = h.results().calls.map((call) => call.result[0]);
    expect(calls.length).toEqual(11);
    expect(calls.slice(0, 5)).toEqual(["A", "B", "C", "D", 1]);
    expect(calls.slice(5, 7).sort()).toEqual([4, 5]);
    expect(calls.slice(7, 9).sort()).toEqual([6, 7]);
    expect(calls.slice(9, 11).sort()).toEqual([2, 3]);
  });

  test("Should pass the remaining capacity to other limiters", async ({ harness: h, track }) => {
    const limiter1 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );
    const limiter2 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );
    const limiter3 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );
    const limiter4 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "busy",
        timeout: 3000,
        maxConcurrent: 3,
        trackDoneStatus: true,
      }),
    );
    let t3, t4;

    await limiter1.schedule({ id: "1" }, h.promise, null, "A");
    await limiter2.schedule({ id: "2" }, h.promise, null, "B");
    await limiter3.schedule({ id: "3" }, h.promise, null, "C");
    await limiter4.schedule({ id: "4" }, h.promise, null, "D");

    // Hold limiter1's job (weight 2) with deferredPromise so the cluster's
    // shared maxConcurrent=3 stays saturated (2+1=3) across the queue
    // count assertions below. With slowPromise(50), under load 4 awaited
    // registrations can take >50ms, so limiter1's job sometimes finished
    // before the asserts ran — capacity freed, limiter3's queued job
    // dispatched, and `limiter3.counts().QUEUED` flipped from 1 to 0.
    //
    // Registration order is load-bearing here (see the capacity-priority
    // comment below), so each schedule is followed by a drain of that
    // limiter's enqueued() barrier — the enqueue-time guarantee the old
    // sequentially-awaited submits provided.
    const sigFirst = deferred();
    const p1 = limiter1.schedule(
      { id: "A", weight: 2 },
      h.deferredPromise,
      sigFirst.signal,
      null,
      1,
    );
    await enqueued(limiter1);
    const p2 = limiter2.schedule({ id: "C" }, h.slowPromise, 550, null, 2);
    await enqueued(limiter2);

    expect(runningOrExecuting(limiter1)).toEqual(1);
    expect(runningOrExecuting(limiter2)).toEqual(1);

    // The .finally callbacks timestamp each job's completion; they are
    // chained at schedule time, before any await of the promises.
    const p3 = limiter3.schedule({ id: "D" }, h.slowPromise, 50, null, 3).finally(() => {
      t3 = Date.now();
    });
    await enqueued(limiter3);
    const p4 = limiter4.schedule({ id: "E" }, h.slowPromise, 50, null, 4).finally(() => {
      t4 = Date.now();
    });
    await enqueued(limiter4);
    const p5 = limiter4.schedule({ id: "G" }, h.slowPromise, 50, null, 5);
    await enqueued(limiter4);

    expect(limiter3.counts().QUEUED).toEqual(1);
    expect(limiter4.counts().QUEUED).toEqual(2);

    // Release limiter1's job; capacity opens up; queued jobs dispatch.
    // Order is preserved because deferredPromise's log.record fires when the
    // signal resolves (matching slowPromise's timing semantics).
    sigFirst.release();

    await Promise.all([p1, p2, p3, p4, p5]);

    // Capacity-priority (process_tick.lua): among clients tied on minimum running
    // load with queued>0, Redis picks the one with the smallest client_last_registered
    // score (oldest registration). Warm-up awaits limiter3.promise before limiter4.promise,
    // so L3’s score stays strictly below L4’s until work runs — the first grant after
    // releaseFirst() targets L3, then FIFO on L4 gives [4] before [5]. Call-log order
    // must remain [[3],[4],[5]]; this is not the symmetric F/G case in "least busy limiter".
    expect(h.log).toHaveCallOrder([["A"], ["B"], ["C"], ["D"], [1], [3], [4], [5], [2]]);

    // limiter3's job 3 and limiter4's job 4 are both 50ms slowJobs that start
    // back-to-back; they should finish near-simultaneously. The 15ms
    // ceiling was too tight under parallel testcontainer load — 100ms
    // still proves "near-simultaneous" while absorbing event-loop jitter.
    expect(Math.abs(t3 - t4)).toBeLessThan(100);
  });

  test("Should take the capacity and blacklist if the priority limiter is not responding", async ({
    harness: h,
    track,
  }) => {
    const limiter1 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "crash",
        timeout: 3000,
        maxConcurrent: 1,
        trackDoneStatus: true,
      }),
    );
    const limiter2 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "crash",
        timeout: 3000,
        maxConcurrent: 1,
        trackDoneStatus: true,
      }),
    );
    const limiter3 = track(
      new Bottleneck({
        datastore: process.env.DATASTORE,
        clearDatastore: true,
        id: "crash",
        timeout: 3000,
        maxConcurrent: 1,
        trackDoneStatus: true,
      }),
    );

    await limiter1.schedule({ id: "1" }, h.promise, null, "A");
    await limiter2.schedule({ id: "2" }, h.promise, null, "B");
    await limiter3.schedule({ id: "3" }, h.promise, null, "C");

    // Registration order is load-bearing (limiter2 must be the priority
    // client when it stops responding), so each schedule is followed by a
    // wait on that limiter's enqueued() barrier — the enqueue-time guarantee the
    // old sequentially-awaited submits provided.
    const p1 = limiter1.schedule({ id: "4" }, h.slowPromise, 100, null, 4);
    await enqueued(limiter1);
    // Job 5's promise never settles: limiter2 disconnects below while the
    // job is still queued, so it is never dispatched nor dropped — no
    // assertion can be attached and it must stay un-awaited.
    limiter2.schedule({ id: "5" }, h.slowPromise, 100, null, 5);
    await enqueued(limiter2);
    const p3 = limiter3.schedule({ id: "6" }, h.slowPromise, 100, null, 6);
    await enqueued(limiter3);
    await limiter2.disconnect(false);

    await Promise.all([p1, p3]);
    expect(h.log).toHaveCallOrder([["A"], ["B"], ["C"], [4], [6]]);
  });
});
