import { describe, expect } from "vitest";
import { useFakeClock } from "./helpers/clock.js";
import { test, waitForState, deferred } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

useFakeClock();

describe("General", () => {
  test("Should prompt to upgrade", ({ makeLimiter }) => {
    const limiter = makeLimiter();
    expect(() => {
      const _limiter = new Bottleneck(1, 250);
    }).toThrow(/Bottleneck v2 takes a single object argument/);
  });

  test("Should allow null capacity", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ id: "null", minTime: 0 });
    await expect(limiter.updateSettings({ minTime: 10 })).resolves.toBe(limiter);
  });

  test("Should keep scope", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });

    class Job {
      constructor() {
        this.value = 5;
      }
      action(x) {
        return this.value + x;
      }
    }
    const job = new Job();

    expect(await limiter.schedule(() => job.action.bind(job)(1))).toEqual(6);
    expect(await limiter.wrap(job.action.bind(job))(2)).toEqual(7);
  });

  test("Should pass multiple arguments back even on errors when using submit()", ({
    harness: h,
    makeLimiter,
  }) => {
    expect.hasAssertions();
    const limiter = makeLimiter({ maxConcurrent: 1 });

    return new Promise((resolve, reject) => {
      limiter.submit(h.job, new Error("welp"), 1, 2, (err, x, y) => {
        try {
          expect(err.message).toEqual("welp");
          expect(x).toEqual(1);
          expect(y).toEqual(2);
          resolve();
        } catch (e) {
          reject(e);
        }
      });
    });
  });

  test("Should expose the Events library", ({ makeLimiter }) => {
    const limiter = makeLimiter();

    class Hello {
      constructor() {
        this.emitter = new Bottleneck.Events(this);
      }

      doSomething() {
        this.emitter.trigger("info", "hello", "world", 123);
        return 5;
      }
    }

    const myObject = new Hello();
    let sawInfo = false;
    myObject.on("info", (...args) => {
      expect(args).toEqual(["hello", "world", 123]);
      sawInfo = true;
    });
    myObject.doSomething();
    expect(sawInfo).toEqual(true);
    expect(myObject.emitter.listenerCount("info")).toEqual(1);
    expect(myObject.emitter.listenerCount("nothing")).toEqual(0);

    myObject.on("blah", "");
    myObject.on("blah", null);
    myObject.on("blah");
    return myObject.emitter.trigger("blah");
  });

  describe("Counts and statuses", () => {
    test("Should check() and return the queued count with and without a priority value", async ({
      harness: h,
      makeLimiter,
    }) => {
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

      // Hold job 1 with a deferred promise so it never finishes until we
      // explicitly release it. Otherwise the prior `slowJob, 50` could finish
      // before all 4 submits complete (each submit adds Redis RTT) and a
      // queued job dispatches, making `queued()` count race the minTime gate.
      const hold1 = deferred();

      expect(await limiter.check()).toEqual(true);

      expect(limiter.queued()).toEqual(0);
      expect(await limiter.clusterQueued()).toEqual(0);

      await limiter.submit({ id: 1 }, h.deferredJob, hold1.signal, null, 1, h.noErrVal(1));
      expect(limiter.queued()).toEqual(0); // It's already running

      expect(await limiter.check()).toEqual(false);

      await limiter.submit({ id: 2 }, h.slowJob, 50, null, 2, h.noErrVal(2));
      expect(limiter.queued()).toEqual(1);
      expect(await limiter.clusterQueued()).toEqual(1);
      expect(limiter.queued(1)).toEqual(0);
      expect(limiter.queued(5)).toEqual(1);

      await limiter.submit({ id: 3 }, h.slowJob, 50, null, 3, h.noErrVal(3));
      expect(limiter.queued()).toEqual(2);
      expect(await limiter.clusterQueued()).toEqual(2);
      expect(limiter.queued(1)).toEqual(0);
      expect(limiter.queued(5)).toEqual(2);

      await limiter.submit({ id: 4 }, h.slowJob, 50, null, 4, h.noErrVal(4));
      expect(limiter.queued()).toEqual(3);
      expect(await limiter.clusterQueued()).toEqual(3);
      expect(limiter.queued(1)).toEqual(0);
      expect(limiter.queued(5)).toEqual(3);

      await limiter.submit({ priority: 1, id: 5 }, h.job, null, 5, h.noErrVal(5));
      expect(limiter.queued()).toEqual(4);
      expect(await limiter.clusterQueued()).toEqual(4);
      expect(limiter.queued(1)).toEqual(1);
      expect(limiter.queued(5)).toEqual(3);

      hold1.release();

      await h.flushLimiter(limiter);
      expect(limiter.queued()).toEqual(0);
      expect(await limiter.clusterQueued()).toEqual(0);
      expect(h.log).toHaveCallOrder([[1], [5], [2], [3], [4]]);
    });

    test("Should return the running and done counts", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 5, minTime: 0 });

      // Held jobs let the test observe each (running, done) checkpoint
      // deterministically. With slowPromise(100) the first checkpoint
      // (running=5) raced Redis RTT — an early job could transition to
      // DONE before the running()/done() round-trip returned, dropping
      // running to 4.
      const hold1 = deferred();
      const hold2 = deferred();
      const hold3 = deferred();

      const [running0, done0] = await Promise.all([limiter.running(), limiter.done()]);
      expect(running0).toEqual(0);
      expect(done0).toEqual(0);

      limiter.submit({ weight: 1, id: 1 }, h.deferredJob, hold1.signal, null, 1, h.noErrVal(1));
      limiter.submit({ weight: 3, id: 2 }, h.deferredJob, hold2.signal, null, 2, h.noErrVal(2));
      limiter.submit({ weight: 1, id: 3 }, h.deferredJob, hold3.signal, null, 3, h.noErrVal(3));
      await limiter.schedule({ weight: 0, id: 4 }, h.promise, null);

      const [running1, done1] = await Promise.all([limiter.running(), limiter.done()]);
      expect(running1).toEqual(5);
      expect(done1).toEqual(0);

      hold1.release();
      hold3.release();
      await waitForState(async () => {
        const [r, d] = await Promise.all([limiter.running(), limiter.done()]);
        expect(r).toBe(3);
        expect(d).toBe(2);
      });

      const [running2, done2] = await Promise.all([limiter.running(), limiter.done()]);
      expect(running2).toEqual(3);
      expect(done2).toEqual(2);

      hold2.release();
      await waitForState(async () => {
        const [r, d] = await Promise.all([limiter.running(), limiter.done()]);
        expect(r).toBe(0);
        expect(d).toBe(5);
      });

      const [running3, done3] = await Promise.all([limiter.running(), limiter.done()]);
      expect(running3).toEqual(0);
      expect(done3).toEqual(5);

      await h.flushLimiter(limiter);
      expect(h.log).toHaveCallOrder([[], [1], [3], [2]]);
    });

    test("Should refuse duplicate Job IDs", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2, minTime: 100, trackDoneStatus: true });

      try {
        await limiter.schedule({ id: "a" }, h.promise, null, 1);
        await limiter.schedule({ id: "b" }, h.promise, null, 2);
        await limiter.schedule({ id: "a" }, h.promise, null, 3);
      } catch (e) {
        expect(e.message).toEqual("A job with the same id already exists (id=a)");
      }
    });

    test("Should return job statuses", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2, minTime: 100 });
      await limiter.ready();

      expect(limiter.counts()).toEqual({ RECEIVED: 0, QUEUED: 0, RUNNING: 0, EXECUTING: 0 });

      const hold1 = deferred();
      const p1 = limiter.schedule({ weight: 1, id: 1 }, h.deferredPromise, hold1.signal, null, 1);
      const p2 = limiter.schedule({ weight: 1, id: 2 }, h.slowPromise, 200, null, 2);
      const p3 = limiter.schedule({ weight: 2, id: 3 }, h.slowPromise, 100, null, 3);
      expect(limiter.counts()).toEqual({ RECEIVED: 3, QUEUED: 0, RUNNING: 0, EXECUTING: 0 });

      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.RUNNING).toBe(1);
        expect(counts.EXECUTING).toBe(1);
      });

      expect(limiter.counts()).toEqual({ RECEIVED: 0, QUEUED: 1, RUNNING: 1, EXECUTING: 1 });
      expect(limiter.jobStatus(1)).toEqual("EXECUTING");
      expect(limiter.jobStatus(2)).toEqual("RUNNING");
      expect(limiter.jobStatus(3)).toEqual("QUEUED");

      hold1.release();
      await h.flushLimiter(limiter);
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
      ]);
      expect(h).toHaveFinalCallAt(400);
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
    });

    test("Should return job statuses, including DONE", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2, minTime: 100, trackDoneStatus: true });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      // Job 1 is held with a deferredPromise so we can deterministically
      // observe the {EXECUTING:1, RUNNING:1, QUEUED:1, DONE:0} state. The
      // original slowPromise(100) raced with minTime=100 — the moment
      // job 2 dispatched (at t=100) was the same instant job 1's 100ms
      // timer was firing, so the predicate could never be true if job 1
      // resolved first (state would jump straight to {DONE:1, EXECUTING:1
      // (job 2 — was RUNNING for one microtask), QUEUED:1}). Holding job 1
      // with deferredPromise eliminates the race.
      const hold1 = deferred();
      const p1 = limiter.schedule({ weight: 1, id: 1 }, h.deferredPromise, hold1.signal, null, 1);
      const p2 = limiter.schedule({ weight: 1, id: 2 }, h.slowPromise, 200, null, 2);
      const p3 = limiter.schedule({ weight: 2, id: 3 }, h.slowPromise, 100, null, 3);
      expect(limiter.counts()).toEqual({
        RECEIVED: 3,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.RUNNING).toBe(1);
        expect(counts.EXECUTING).toBe(1);
        expect(counts.DONE).toBe(0);
      });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 1,
        RUNNING: 1,
        EXECUTING: 1,
        DONE: 0,
      });
      expect(limiter.jobStatus(1)).toEqual("EXECUTING");
      expect(limiter.jobStatus(2)).toEqual("RUNNING");
      expect(limiter.jobStatus(3)).toEqual("QUEUED");

      hold1.release();

      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.RUNNING).toBe(0);
        expect(counts.EXECUTING).toBe(1);
        expect(counts.DONE).toBe(1);
      });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 1,
        RUNNING: 0,
        EXECUTING: 1,
        DONE: 1,
      });
      expect(limiter.jobStatus(1)).toEqual("DONE");
      expect(limiter.jobStatus(2)).toEqual("EXECUTING");
      expect(limiter.jobStatus(3)).toEqual("QUEUED");

      await h.flushLimiter(limiter);
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
      ]);

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 4,
      });
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
    });

    test("Should return jobs for a status", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2, minTime: 100, trackDoneStatus: true });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      // Job 1 is held with a deferredJob so we can observe the state where
      // job 1 is EXECUTING, job 2 is RUNNING (just dispatched), job 3 is
      // QUEUED, and DONE=0 deterministically. Using slowPromise(100) here
      // races with minTime=100 — the moment job 2 dispatches is the same
      // instant job 1 finishes, so the {DONE:0, EXECUTING:1, RUNNING:1}
      // window may not exist depending on microtask order.
      const hold1 = deferred();

      limiter.submit({ weight: 1, id: 1 }, h.deferredJob, hold1.signal, null, 1, h.noErrVal(1));
      const p2 = limiter.schedule({ weight: 1, id: 2 }, h.slowPromise, 200, null, 2);
      const p3 = limiter.schedule({ weight: 2, id: 3 }, h.slowPromise, 100, null, 3);
      expect(limiter.counts()).toEqual({
        RECEIVED: 3,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      expect(limiter.jobs()).toEqual(["1", "2", "3"]);
      expect(limiter.jobs("RECEIVED")).toEqual(["1", "2", "3"]);

      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.RUNNING).toBe(1);
        expect(counts.EXECUTING).toBe(1);
        expect(counts.DONE).toBe(0);
      });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 1,
        RUNNING: 1,
        EXECUTING: 1,
        DONE: 0,
      });
      expect(limiter.jobs("EXECUTING")).toEqual(["1"]);
      expect(limiter.jobs("RUNNING")).toEqual(["2"]);
      expect(limiter.jobs("QUEUED")).toEqual(["3"]);

      hold1.release();

      // After hold1.release(), job 1 transitions to DONE and frees a slot. Job 2 is
      // already in RUNNING and immediately moves to EXECUTING. Wait for that
      // to complete to avoid catching the brief in-between RUNNING=1 state.
      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.DONE).toBe(1);
        expect(counts.EXECUTING).toBe(1);
        expect(counts.RUNNING).toBe(0);
      });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 1,
        RUNNING: 0,
        EXECUTING: 1,
        DONE: 1,
      });
      expect(limiter.jobs("DONE")).toEqual(["1"]);
      expect(limiter.jobs("EXECUTING")).toEqual(["2"]);
      expect(limiter.jobs("QUEUED")).toEqual(["3"]);

      await h.flushLimiter(limiter);
      await Promise.all([expect(p2).resolves.toEqual([2]), expect(p3).resolves.toEqual([3])]);

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 4,
      });
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
    });

    test("Should trigger events on status changes", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2, minTime: 100, trackDoneStatus: true });
      await limiter.ready();
      let onReceived = 0;
      let onQueued = 0;
      let onScheduled = 0;
      let onExecuting = 0;
      let onDone = 0;
      limiter.on("received", (info) => {
        expect(Object.keys(info).sort()).toEqual(["args", "options"]);
        onReceived++;
      });
      limiter.on("queued", (info) => {
        expect(Object.keys(info).sort()).toEqual(["args", "blocked", "options", "reachedHWM"]);
        onQueued++;
      });
      limiter.on("scheduled", (info) => {
        expect(Object.keys(info).sort()).toEqual(["args", "options"]);
        onScheduled++;
      });
      limiter.on("executing", (info) => {
        expect(Object.keys(info).sort()).toEqual(["args", "options", "retryCount"]);
        onExecuting++;
      });
      limiter.on("done", (info) => {
        expect(Object.keys(info).sort()).toEqual(["args", "options", "retryCount"]);
        onDone++;
      });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      const hold1 = deferred();
      const p1 = limiter.schedule({ weight: 1, id: 1 }, h.deferredPromise, hold1.signal, null, 1);
      const p2 = limiter.schedule({ weight: 1, id: 2 }, h.slowPromise, 200, null, 2);
      const p3 = limiter.schedule({ weight: 2, id: 3 }, h.slowPromise, 100, null, 3);
      expect(limiter.counts()).toEqual({
        RECEIVED: 3,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([3, 0, 0, 0, 0]);

      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.RUNNING).toBe(1);
        expect(counts.EXECUTING).toBe(1);
        expect(counts.DONE).toBe(0);
      });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 1,
        RUNNING: 1,
        EXECUTING: 1,
        DONE: 0,
      });
      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([3, 3, 2, 1, 0]);

      hold1.release();

      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.RUNNING).toBe(0);
        expect(counts.EXECUTING).toBe(1);
        expect(counts.DONE).toBe(1);
      });

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 1,
        RUNNING: 0,
        EXECUTING: 1,
        DONE: 1,
      });
      expect(limiter.jobs("DONE")).toEqual(["1"]);
      expect(limiter.jobs("EXECUTING")).toEqual(["2"]);
      expect(limiter.jobs("QUEUED")).toEqual(["3"]);
      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([3, 3, 2, 2, 1]);

      await h.flushLimiter(limiter);
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
      ]);

      expect(limiter.counts()).toEqual({
        RECEIVED: 0,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 4,
      });
      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([4, 4, 4, 4, 4]);
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
    });
  });

  describe("Events", () => {
    test("Should return itself", ({ makeLimiter }) => {
      const limiter = makeLimiter({ id: "test-limiter" });

      const returned = limiter.on("ready", () => {});
      // The contract is that `.on()` returns the limiter itself for chaining;
      // compare to `limiter.id` rather than the literal "test-limiter" so this
      // works in Redis projects where test/bottleneck.js prefixes ids per fork.
      expect(returned.id).toEqual(limiter.id);
    });

    test("Should fire events on empty queue", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });
      let calledEmpty = 0;
      let calledIdle = 0;
      let calledDepleted = 0;

      limiter.on("empty", () => {
        calledEmpty++;
      });
      limiter.on("idle", () => {
        calledIdle++;
      });
      limiter.on("depleted", () => {
        calledDepleted++;
      });

      await expect(limiter.schedule({ id: 1 }, h.slowPromise, 50, null, 1)).resolves.toEqual([1]);
      expect(calledEmpty).toEqual(1);
      expect(calledIdle).toEqual(1);
      await Promise.all([
        expect(limiter.schedule({ id: 2 }, h.slowPromise, 50, null, 2)).resolves.toEqual([2]),
        expect(limiter.schedule({ id: 3 }, h.slowPromise, 50, null, 3)).resolves.toEqual([3]),
      ]);
      await limiter.submit({ id: 4 }, h.slowJob, 50, null, 4, null);
      expect(h).toHaveFinalCallAt(250);
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
      expect(calledEmpty).toEqual(3);
      expect(calledIdle).toEqual(2);
      expect(calledDepleted).toEqual(0);
      await h.flushLimiter(limiter);
    });

    test("Should fire events once", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });
      let calledEmptyOnce = 0;
      let calledIdleOnce = 0;
      let calledEmpty = 0;
      let calledIdle = 0;
      let calledDepleted = 0;

      limiter.once("empty", () => {
        calledEmptyOnce++;
      });
      limiter.once("idle", () => {
        calledIdleOnce++;
      });
      limiter.on("empty", () => {
        calledEmpty++;
      });
      limiter.on("idle", () => {
        calledIdle++;
      });
      limiter.on("depleted", () => {
        calledDepleted++;
      });

      const p1 = limiter.schedule(h.slowPromise, 50, null, 1);

      await expect(limiter.schedule(h.promise, null, 2)).resolves.toEqual([2]);
      await expect(p1).resolves.toEqual([1]);
      expect(calledEmptyOnce).toEqual(1);
      expect(calledIdleOnce).toEqual(1);
      expect(calledEmpty).toEqual(1);
      expect(calledIdle).toEqual(1);
      await expect(limiter.schedule(h.promise, null, 3)).resolves.toEqual([3]);
      expect(h).toHaveFinalCallAt(200);
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
      expect(calledEmptyOnce).toEqual(1);
      expect(calledIdleOnce).toEqual(1);
      expect(calledEmpty).toEqual(2);
      expect(calledIdle).toEqual(2);
      expect(calledDepleted).toEqual(0);
    });

    test("Should support faulty event listeners", ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 }, { expectErrors: true });
      // We only care that the listener-thrown error eventually surfaces on
      // the "error" event. Counting calls is brittle under Redis-backed runs
      // because a connectTimeout retry (see test/redis-client-options.js) can
      // emit an unrelated ETIMEDOUT first, which would otherwise pre-increment
      // any counter and starve the resolve condition.
      let fired = false;
      const errored = new Promise((resolve) => {
        limiter.on("error", (err) => {
          if (err.message === "Oh noes!" && !fired) {
            fired = true;
            resolve();
          }
        });
      });
      limiter.on("empty", () => {
        throw new Error("Oh noes!");
      });

      return Promise.all([
        expect(limiter.schedule(h.promise, null, 1)).resolves.toEqual([1]),
        errored,
      ]);
    });

    test("Should wait for async event listeners", ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 }, { expectErrors: true });
      // Match on the specific error message — under load any unrelated redis
      // error could fire first; we only care that "It broke!" eventually does.
      let fired = false;
      const errored = new Promise((resolve) => {
        limiter.on("error", (err) => {
          if (err.message === "It broke!" && !fired) {
            fired = true;
            resolve();
          }
        });
      });
      limiter.on("empty", async () => {
        const x = await h.slowPromise(100, null, 1, 2);
        expect(x).toEqual([1, 2]);
        throw new Error("It broke!");
      });

      return Promise.all([
        expect(limiter.schedule(h.promise, null, 1)).resolves.toEqual([1]),
        errored,
      ]);
    });
  });
});
