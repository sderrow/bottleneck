import { describe, expect } from "vitest";
import type BottleneckBase from "../src/Bottleneck";
import * as Scripts from "../src/cluster/Scripts";
import sleep from "../src/sleep";
import Bottleneck from "./bottleneck";
import { test, waitForState, deferred, enqueued } from "./helpers/test-api";

// Causality policy (Workstream B): observe product-timer effects via waitForState
// and state counts — never assert wall-clock bounds around real network time.

type Limiter = BottleneckBase;
const limiterKeys = (limiter: Limiter): any[] =>
  Scripts.allKeys((limiter._store as any).originalId);
const countKeys = (limiter: Limiter) => runCommand(limiter, "exists", limiterKeys(limiter));
const deleteKeys = (limiter: Limiter) => runCommand(limiter, "del", limiterKeys(limiter));
const runCommand = (limiter: Limiter, command: string, args: string[]) =>
  (limiter._store as any).connection.__runCommand__([command, ...args]);
const runningOrExecuting = (limiter: Limiter) => {
  const counts = limiter.counts();
  return counts.RUNNING + counts.EXECUTING;
};

// Error messages changed between redis 6 and 7, with a new ERR prefix
const SETTINGS_KEY_NOT_FOUND = /^(.*\s)?SETTINGS_KEY_NOT_FOUND$/;
const UNKNOWN_CLIENT = /^(.*\s)?UNKNOWN_CLIENT$/;

async function captureFirstScriptError(limiter: Limiter, trigger: () => unknown) {
  const connection = (limiter._store as any).connection;
  const original = connection.__runScript__.bind(connection);
  let captured: any;

  connection.__runScript__ = async (name: string, id: string, args: unknown[]) => {
    try {
      return await original(name, id, args);
    } catch (e) {
      captured ??= e;
      throw e;
    }
  };

  try {
    await trigger();
  } finally {
    connection.__runScript__ = original;
  }

  return captured;
}

/**
 * Record which limiters receive `capacity-priority` events — emitted by the
 * datastore when a capacity grant targets that client — in firing order.
 * Each limiter is tagged with its 1-based argument position.
 */
function captureCapacityPriorityTargets(...limiters: Limiter[]) {
  const targeted: number[] = [];
  limiters.forEach((limiter: Limiter, i: number) => {
    limiter.on("capacity-priority", () => targeted.push(i + 1));
  });
  return targeted;
}
describe("Cluster coordination", () => {
  if (process.env.DATASTORE !== "redis" && process.env.DATASTORE !== "ioredis") {
    throw new Error("DATASTORE must be redis or ioredis");
  }

  test("Should chain local and distributed limiters (total concurrency)", async ({
    harness: h,
    makeLimiter,
  }) => {
    const rootLimiter = makeLimiter({ id: "limiter1", maxConcurrent: 3 });
    const limiter2 = makeLimiter({ id: "limiter2", maxConcurrent: 1, datastore: "local" });
    const limiter3 = makeLimiter({ id: "limiter3", maxConcurrent: 2, datastore: "local" });

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
  }) => {
    const rootLimiter = makeLimiter({ maxConcurrent: 2 });
    const limiter2 = makeLimiter({ maxConcurrent: 1, datastore: "local" });
    const limiter3 = makeLimiter({ maxConcurrent: 2, datastore: "local" });

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

  test("Should use the limiter ID to build Redis keys", async ({ makeLimiter }) => {
    const rootLimiter = makeLimiter();
    const randomId = rootLimiter._randomIndex();
    const limiter = makeLimiter({
      id: randomId,
      clearDatastore: true,
    });

    await limiter.ready();
    const keys = limiterKeys(limiter);
    keys.forEach((key) => expect(key.indexOf(randomId)).toBeGreaterThan(0));
    const deleted = await deleteKeys(limiter);
    expect(deleted).toEqual(5);
  });

  test("Should not fail when Redis data is missing", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ clearDatastore: true });

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

  test("Should parse SETTINGS_KEY_NOT_FOUND from Redis", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ clearDatastore: true });

    await limiter.ready();
    await deleteKeys(limiter);

    const err = await captureFirstScriptError(limiter, () => limiter.running());

    expect(err).toBeTruthy();
    expect(err.message).toMatch(SETTINGS_KEY_NOT_FOUND);
  });

  test("Should re-register when client registration is missing in Redis", async ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ clearDatastore: true });

    await limiter.ready();
    const clientLastSeenKey = limiterKeys(limiter)[7];
    const clientId = limiter._store.clientId;
    await runCommand(limiter, "zrem", [clientLastSeenKey, clientId]);

    expect(await limiter.running()).toEqual(0);
    expect(await runCommand(limiter, "zscore", [clientLastSeenKey, clientId])).not.toBeNull();
  });

  test("Should parse UNKNOWN_CLIENT from Redis", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ clearDatastore: true });

    await limiter.ready();
    const clientLastSeenKey = limiterKeys(limiter)[7];
    await runCommand(limiter, "zrem", [clientLastSeenKey, limiter._store.clientId]);

    const err = await captureFirstScriptError(limiter, () => limiter.running());

    expect(err).toBeTruthy();
    expect(err.message).toMatch(UNKNOWN_CLIENT);
  });

  test("Should drop all jobs in the Cluster when entering blocked mode", async ({
    harness: h,
    makeLimiter,
  }) => {
    const rootLimiter = makeLimiter();
    const limiter1 = makeLimiter({
      id: "blocked",
      trackDoneStatus: true,
      clearDatastore: true,
      maxConcurrent: 1,
      minTime: 50,
      highWater: 2,
      strategy: Bottleneck.strategy.BLOCK,
    });
    const client_num_queued_key = limiterKeys(limiter1)[5];

    await limiter1.ready();
    const limiter2 = makeLimiter({
      id: "blocked",
      trackDoneStatus: true,
      clearDatastore: false,
    });
    await limiter2.ready();

    // Fire jobs 1-2 on limiter1, then wait for enqueued(limiter1) so both
    // registrations reach redis before limiter2's jobs — preserving the
    // enqueue order the old awaited submit groups pinned. Job 2 is NOT
    // dropped yet at this point: blocked mode only trips once limiter2's
    // submissions push the cluster queue to highWater, so p2's rejection
    // cannot be awaited before jobs 3-5 are fired.
    //
    // Job 1 is held open by a deferred signal (never a real-timer
    // slowPromise): with maxConcurrent 1 the cluster stays saturated while
    // jobs 2-5 register, so nothing can dispatch mid-submission. The old
    // 100ms slowPromise raced its own execution window — slow registration
    // round-trips under load could outlast job 1 (and even job 2), and the
    // freed capacity could be granted to limiter2's queued job 3
    // (cross-client dispatch order is not FIFO). Job 3 — EXECUTING rather
    // than queued when the block trips — is exempt from the drop and
    // resolved [3] instead of rejecting.
    const sig1 = deferred();
    const p1 = limiter1.schedule(h.deferredPromise, sig1.signal, null, 1);
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
    // All four drops are confirmed; free job 1's slot. It was already
    // EXECUTING when the block tripped, so it completes normally.
    sig1.release();

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

  test("Should pass messages to all limiters in Cluster", async ({ makeLimiter }) => {
    const rootLimiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      id: "super-duper",
    });
    const limiter1 = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      id: "super-duper",
    });
    const limiter2 = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      id: "nope",
    });
    const received: any[] = [];

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

    limiter1.publish(555 as unknown as string);
    await waitForState(() => {
      expect(received.length).toBeGreaterThanOrEqual(4);
    });

    limiter1.disconnect();
    limiter2.disconnect();
    expect(received.sort()).toEqual([1, 2, "555", "555"]);
  });

  test("Should pass messages to correct limiter after Group re-instantiations", async ({
    makeGroup,
  }) => {
    const group = makeGroup({
      maxConcurrent: 1,
      minTime: 100,
      datastore: process.env.DATASTORE,
    });
    const received: any[] = [];

    await new Promise<void>((resolve, _reject) => {
      const limiter = group.key("A");

      limiter.on("message", (msg) => {
        received.push("1", msg);
        return resolve();
      });
      limiter.publish("Bonjour!");
    });

    await new Promise<void>((resolve, _reject) => {
      const limiter = group.key("B");

      limiter.on("message", (msg) => {
        received.push("2", msg);
        return resolve();
      });
      limiter.publish("Comment allez-vous?");
    });

    await group.deleteKey("A");

    await new Promise<void>((resolve, _reject) => {
      const limiter = group.key("A");

      limiter.on("message", (msg) => {
        received.push("3", msg);
        return resolve();
      });
      limiter.publish("Au revoir!");
    });

    expect(received).toEqual(["1", "Bonjour!", "2", "Comment allez-vous?", "3", "Au revoir!"]);
    // Semantic, not cleanup: flush=true gracefully drains the un-awaited
    // "Au revoir!" PUBLISH reply before closing. makeGroup's teardown
    // disconnect(false) would destroy the socket mid-flight and reject that pending command.
    group.disconnect();
  });

  test("Should have a default key TTL when using Groups", async ({ makeGroup }) => {
    const group = makeGroup({
      datastore: process.env.DATASTORE,
    });

    await group.key("one").ready();
    const limiter = group.key("one");
    const settings_key = limiterKeys(limiter)[0];
    const ttl = await runCommand(limiter, "ttl", [settings_key]);
    expect(ttl).toBeGreaterThanOrEqual(290);
    expect(ttl).toBeLessThanOrEqual(305);
  });

  test("Should support Groups and expire Redis keys", async ({ makeLimiter, makeGroup }) => {
    const rootLimiter = makeLimiter();
    const group = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      minTime: 50,
      timeout: 200,
    });

    const t0 = Date.now();
    const results: Record<string, number> = {};
    const job = (x: string) => {
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
    expect(results.a!).toBeLessThanOrEqual(results.b!);
    expect(results.b!).toBeLessThanOrEqual(results.c!);
    expect(results.d!).toBeLessThanOrEqual(results.e!);

    // Different limiters in the same group should dispatch in parallel
    // (no shared minTime/maxConcurrent). Tolerate dispatch jitter —
    // simultaneous dispatches across separate limiters drift slightly
    // under load even though the intended behavior is "fire together".
    expect(Math.abs(results.a! - results.d!)).toBeLessThanOrEqual(100);
    expect(Math.abs(results.d! - results.f!)).toBeLessThanOrEqual(100);
    expect(Math.abs(results.b! - results.e!)).toBeLessThanOrEqual(100);

    // Poll for autocleanup AND the underlying disconnect to settle.
    // group.deleteKey removes from instances synchronously but awaits
    // instance.disconnect() which clears connection.limiters async — both
    // need to be flushed before the assertions below.
    await waitForState(() => {
      expect(group.keys().length).toBe(0);
      expect(Object.keys((group.connection as any).limiters).length).toBe(0);
    });

    const countsAfterCleanup = await Promise.all([
      countKeys(limiter1),
      countKeys(limiter2),
      countKeys(limiter3),
    ]);
    expect(countsAfterCleanup).toEqual([0, 0, 0]);
    expect(group.keys().length).toEqual(0);
    expect(Object.keys((group.connection as any).limiters).length).toEqual(0);
  });

  test("Should not recreate a key when running heartbeat", async ({ harness: h, makeGroup }) => {
    const group = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      maxConcurrent: 50,
      minTime: 50,
      timeout: 300,
      heartbeatInterval: 5,
    });
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
    makeGroup,
  }) => {
    // Bump timeout (and the corresponding waitForState below) so autocleanup
    // doesn't race with the initial schedule under stress. Original 300ms
    // gave a 150ms autocleanup interval that could fire before init.lua
    // settled when redis was slow.
    const groupTimeout = 5000;
    const group1 = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      maxConcurrent: 50,
      minTime: 50,
      timeout: groupTimeout,
    });
    const group2 = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      maxConcurrent: 50,
      minTime: 50,
      timeout: groupTimeout,
    });
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
    makeGroup,
  }) => {
    // groupTimeout pulls double duty here: it sets the redis-side TTL
    // (must not expire before group2.deleteKey runs), and it gates
    // autocleanup interval (timeout/2). 2000ms was enough for autocleanup
    // to fire within the waitFor window, but tight enough that under stress
    // the keys could TTL-expire before deleteKey ran. Refreshing the TTL
    // explicitly via running() right before deleteKey decouples the two.
    const groupTimeout = 5000;
    const group1 = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      maxConcurrent: 50,
      minTime: 50,
      timeout: groupTimeout,
    });
    const group2 = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      maxConcurrent: 50,
      minTime: 50,
      timeout: groupTimeout,
    });
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

  test("Should returns all Group keys in the cluster", async ({ makeGroup }) => {
    // Use a long timeout so redis-side TTLs cannot expire mid-test under load.
    // Original 3000ms was tight enough that a slow run (cumulative redis latency)
    // could let keys expire before the assertions, then autocleanup would prune
    // them from instances and group.keys() would surprisingly return [].
    const group1 = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      id: "same",
      timeout: 30000,
    });
    const group2 = makeGroup({
      datastore: process.env.DATASTORE,
      clearDatastore: true,
      id: "same",
      timeout: 30000,
    });
    const keys1 = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur"];
    const keys2 = ["adipiscing", "elit"];
    const both = keys1.concat(keys2);

    await Promise.all(keys1.map((k) => group1.key(k).ready()));
    await Promise.all(keys2.map((k) => group2.key(k).ready()));

    expect(group1.keys().sort()).toEqual(keys1.sort());
    expect(group2.keys().sort()).toEqual(keys2.sort());
    expect((await group1.clusterKeys()).sort()).toEqual(both.sort());
    expect((await group1.clusterKeys()).sort()).toEqual(both.sort());

    const group3 = makeGroup({ datastore: "local" });
    expect(await group3.clusterKeys()).toEqual([]);
  });

  test("Should queue up the least busy limiter", async ({ harness: h, makeLimiter }) => {
    const busyOpts = {
      clearDatastore: true,
      id: "busy",
      timeout: 3000,
      maxConcurrent: 3,
      trackDoneStatus: true,
    };
    const limiter1 = makeLimiter(busyOpts);
    const limiter2 = makeLimiter(busyOpts);
    const limiter3 = makeLimiter(busyOpts);
    const limiter4 = makeLimiter(busyOpts);

    await limiter1.schedule({ id: "1" }, h.promise, null, "A");
    await limiter2.schedule({ id: "2" }, h.promise, null, "B");
    await limiter3.schedule({ id: "3" }, h.promise, null, "C");
    await limiter4.schedule({ id: "4" }, h.promise, null, "D");

    // Hold A open with a deferred job so the cluster's shared maxConcurrent=3
    // stays saturated (A+B+C) while D/E/F/G queue, regardless of how long the
    // registration round-trips take. Released after the QUEUED assertions so
    // it finishes before B.
    const sigA = deferred();

    const p1 = limiter1.schedule({ id: "A" }, h.deferredPromise, sigA.signal, null, 1);
    await enqueued(limiter1);
    // B and C must finish after D/E/F/G. Use generous durations so the test
    // is robust to redis round-trip delays between release and completion.
    const p2 = limiter1.schedule({ id: "B" }, h.slowPromise, 1000, null, 2);
    await enqueued(limiter1);
    const p3 = limiter2.schedule({ id: "C" }, h.slowPromise, 1050, null, 3);
    await enqueued(limiter2);

    expect(runningOrExecuting(limiter1)).toEqual(2);
    expect(runningOrExecuting(limiter2)).toEqual(1);

    const p4 = limiter3.schedule({ id: "D" }, h.slowPromise, 50, null, 4);
    const p5 = limiter4.schedule({ id: "E" }, h.slowPromise, 50, null, 5);
    const p6 = limiter3.schedule({ id: "F" }, h.slowPromise, 50, null, 6);
    const p7 = limiter4.schedule({ id: "G" }, h.slowPromise, 50, null, 7);
    await Promise.all([enqueued(limiter3), enqueued(limiter4)]);

    expect(limiter3.counts().QUEUED).toEqual(2);
    expect(limiter4.counts().QUEUED).toEqual(2);

    sigA.release();

    await Promise.all([p1, p2, p3, p4, p5, p6, p7]);

    // The contract "the least-busy client WINS each capacity grant" is
    // asserted deterministically at the broadcast level in the two
    // capacity-priority tests below. End to end, the heartbeat's plain
    // `capacity:` broadcasts (process_tick always_publish) turn every grant
    // into a first-come free-for-all among clients with queued work — the
    // product guarantees least-busy distribution only on a best-effort basis
    // — so this e2e test pins what IS guaranteed no matter who wins each
    // slot:
    //   - Warm-ups [A,B,C,D] then job [1] first (D-G queue while saturated).
    //   - All four 50ms jobs ran, before the 1000/1050ms jobs.
    //   - Per-limiter FIFO: 4 before 6 (limiter3), 5 before 7 (limiter4) —
    //     each client drains its own queue in order no matter which slots
    //     it wins.
    const calls = h.results().calls.map((call) => (call.result as any)[0]);
    expect(calls.length).toEqual(11);
    expect(calls.slice(0, 5)).toEqual(["A", "B", "C", "D", 1]);
    expect(calls.slice(5, 9).sort()).toEqual([4, 5, 6, 7]);
    expect(calls.slice(9, 11).sort()).toEqual([2, 3]);
    expect(calls.indexOf(4)).toBeLessThan(calls.indexOf(6));
    expect(calls.indexOf(5)).toBeLessThan(calls.indexOf(7));
  });

  test("Should pass the remaining capacity to other limiters", async ({
    harness: h,
    makeLimiter,
  }) => {
    const busyOpts = {
      clearDatastore: true,
      id: "busy",
      timeout: 3000,
      maxConcurrent: 3,
      trackDoneStatus: true,
    };
    const limiter1 = makeLimiter(busyOpts);
    const limiter2 = makeLimiter(busyOpts);
    const limiter3 = makeLimiter(busyOpts);
    const limiter4 = makeLimiter(busyOpts);

    await limiter1.schedule({ id: "1" }, h.promise, null, "A");
    await limiter2.schedule({ id: "2" }, h.promise, null, "B");
    await limiter3.schedule({ id: "3" }, h.promise, null, "C");
    await limiter4.schedule({ id: "4" }, h.promise, null, "D");

    // Hold limiter1's job (weight 2) with deferredPromise so the cluster's
    // shared maxConcurrent=3 stays saturated (2+1=3) across the queue count
    // assertions below — with slowPromise(50) on job 2, slow registrations
    // under load could otherwise let it finish before the asserts ran.
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

    const p3 = limiter3.schedule({ id: "D" }, h.slowPromise, 50, null, 3);
    await enqueued(limiter3);
    const p4 = limiter4.schedule({ id: "E" }, h.slowPromise, 50, null, 4);
    const p5 = limiter4.schedule({ id: "G" }, h.slowPromise, 50, null, 5);
    await enqueued(limiter4);

    expect(limiter3.counts().QUEUED).toEqual(1);
    expect(limiter4.counts().QUEUED).toEqual(2);

    // Release limiter1's job; capacity opens up; queued jobs dispatch.
    // Order is preserved because deferredPromise's log.record fires when the
    // signal resolves (matching slowPromise's timing semantics).
    sigFirst.release();

    await Promise.all([p1, p2, p3, p4, p5]);

    // Which client wins each freed slot is a best-effort, first-come
    // free-for-all under heartbeat broadcasts (see "least busy limiter"), so
    // the strict [3] before [4] grant order is NOT asserted here — the
    // capacity-priority tiebreak is covered at the broadcast level below.
    // What is guaranteed: warm-ups + [1] first, all three 50ms jobs ran
    // before the 550ms job, and limiter4's own FIFO ([4] before [5]).
    const calls = h.results().calls.map((call) => (call.result as any)[0]);
    expect(calls.length).toEqual(9);
    expect(calls.slice(0, 5)).toEqual(["A", "B", "C", "D", 1]);
    expect(calls.slice(5, 8).sort()).toEqual([3, 4, 5]);
    expect(calls[8]).toEqual(2);
    expect(calls.indexOf(4)).toBeLessThan(calls.indexOf(5));
  });

  test("Should take the capacity and blacklist if the priority limiter is not responding", async ({
    harness: h,
    makeLimiter,
  }) => {
    const crashOpts = {
      clearDatastore: true,
      id: "crash",
      timeout: 3000,
      maxConcurrent: 1,
      trackDoneStatus: true,
    };
    const limiter1 = makeLimiter(crashOpts);
    const limiter2 = makeLimiter(crashOpts);
    const limiter3 = makeLimiter(crashOpts);

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

  // The deterministic core of least-busy dispatch is the tiebreak inside
  // process_tick.lua: among responsive clients with queued>0, prefer the
  // lowest client_running, then the oldest client_last_registered. The
  // decision is made synchronously inside the script that frees the
  // capacity, and the targeted client's datastore emits a `capacity-priority`
  // event when the grant message arrives — so asserting WHICH limiter's
  // event fired pins the decision without touching the pubsub wire format,
  // and is immune to the heartbeat `capacity:` free-for-all that makes
  // end-to-end dispatch order untestable (see "least busy limiter" above).
  // Ordering note: the losing candidate can only be targeted by a LATER
  // grant — one whose releasing completion strictly follows the winner's
  // dispatch — so the winner's event always fires first. Heartbeats run at
  // production defaults here — that is the point.

  test("Should target capacity-priority grants at the oldest registration when running is tied", async ({
    harness: h,
    makeLimiter,
  }) => {
    const opts = { clearDatastore: true, id: "cap-tie", maxConcurrent: 2 };
    const limiter1 = makeLimiter(opts);
    const limiter2 = makeLimiter(opts);
    const limiter3 = makeLimiter(opts);
    const limiter4 = makeLimiter(opts);
    const targeted = captureCapacityPriorityTargets(limiter1, limiter2, limiter3, limiter4);

    // Sequential warm-ups pin client_last_registered order (L1 < L2 < L3 <
    // L4); the score refreshes only on dispatch (register.lua).
    await limiter1.schedule(h.promise, null, "w1");
    await limiter2.schedule(h.promise, null, "w2");
    await limiter3.schedule(h.promise, null, "w3");
    await limiter4.schedule(h.promise, null, "w4");

    // Saturate the cluster with two held jobs, then queue one job on each of
    // L3 and L4 — both candidates sit at running 0 with pinned scores.
    const sig1 = deferred();
    const sig2 = deferred();
    const p1 = limiter1.schedule(h.deferredPromise, sig1.signal, null, 1);
    const p2 = limiter2.schedule(h.deferredPromise, sig2.signal, null, 2);
    await Promise.all([enqueued(limiter1), enqueued(limiter2)]);

    const p3 = limiter3.schedule(h.promise, null, 3);
    const p4 = limiter4.schedule(h.promise, null, 4);
    await Promise.all([enqueued(limiter3), enqueued(limiter4)]);

    // Freeing a slot decides the grant inside L1's free.lua: L3 and L4 tie
    // on running, L3's registration is older, so L3 is targeted. A heartbeat
    // broadcast may still steal the actual dispatch afterwards — the
    // targeting decision is what this test pins.
    sig1.release();
    await waitForState(() => expect(targeted.length).toBeGreaterThanOrEqual(1));
    expect(targeted[0]).toEqual(3);

    sig2.release();
    await Promise.all([p1, p2, p3, p4]);
  });

  test("Should target capacity-priority grants at the least busy client even with a newer registration", async ({
    harness: h,
    makeLimiter,
  }) => {
    const opts = { clearDatastore: true, id: "cap-running", maxConcurrent: 3 };
    const limiter1 = makeLimiter(opts);
    const limiter2 = makeLimiter(opts);
    const limiter3 = makeLimiter(opts);
    const limiter4 = makeLimiter(opts);
    const targeted = captureCapacityPriorityTargets(limiter1, limiter2, limiter3, limiter4);

    // Warm-ups pin scores L1 < L2 < L3 < L4; the extra dispatch on L3 makes
    // ITS score the newest, so a score-primary rule would pick L4 below.
    // This is what makes the assertion discriminate running-first from
    // registration-first.
    await limiter1.schedule(h.promise, null, "w1");
    await limiter2.schedule(h.promise, null, "w2");
    await limiter3.schedule(h.promise, null, "w3");
    await limiter4.schedule(h.promise, null, "w4");
    await limiter3.schedule(h.promise, null, "w3b");

    // Saturate with three held jobs — L4's holder leaves it at running 1.
    const sig1 = deferred();
    const sig2 = deferred();
    const sig4 = deferred();
    const p1 = limiter1.schedule(h.deferredPromise, sig1.signal, null, 1);
    const p2 = limiter2.schedule(h.deferredPromise, sig2.signal, null, 2);
    const p4 = limiter4.schedule(h.deferredPromise, sig4.signal, null, 4);
    await Promise.all([enqueued(limiter1), enqueued(limiter2), enqueued(limiter4)]);

    // Queue one job on each of L3 (running 0) and L4 (running 1).
    const p3 = limiter3.schedule(h.promise, null, 3);
    const p5 = limiter4.schedule(h.promise, null, 5);
    await Promise.all([enqueued(limiter3), enqueued(limiter4)]);

    // Freeing a slot: L3 (running 0, newest score) vs L4 (running 1, older
    // score) — running-primary must pick L3.
    sig1.release();
    await waitForState(() => expect(targeted.length).toBeGreaterThanOrEqual(1));
    expect(targeted[0]).toEqual(3);

    sig2.release();
    sig4.release();
    await Promise.all([p1, p2, p3, p4, p5]);
  });
});
