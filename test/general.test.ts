import { describe, expect } from "vitest";
import type BottleneckBase from "../src/Bottleneck";
import Bottleneck from "./bottleneck";
import { useFakeClock } from "./helpers/clock";
import { test, waitForState, deferred, enqueued } from "./helpers/test-api";

useFakeClock();

// White-box helper: swallows the datastore-disconnect noise that fires when a
// limiter is shutting down, but surfaces everything else.
const disconnectError = () => {
  const e = new Error("connection is closed");
  e.constructor = { name: "DisconnectsClientError" } as any;
  return e;
};

describe("General", () => {
  test("Should prompt to upgrade", () => {
    expect(() => {
      const _limiter = new (Bottleneck as any)(1, 250);
    }).toThrow(/Bottleneck v2 takes a single object argument/);
  });

  test("Should allow null capacity", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ id: "null", minTime: 0 });
    await expect(limiter.updateSettings({ minTime: 10 })).resolves.toBe(limiter);
  });

  test("Should keep scope", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1 });

    class Job {
      value: number;
      constructor() {
        this.value = 5;
      }
      action(x: number) {
        return this.value + x;
      }
    }
    const job = new Job();

    expect(await limiter.schedule(() => job.action.bind(job)(1))).toEqual(6);
    expect(await limiter.wrap(job.action.bind(job))(2)).toEqual(7);
  });

  test("Should expose the Events library", () => {
    class Hello {
      emitter: InstanceType<typeof Bottleneck.Events>;
      // Installed onto the instance by the Events constructor at runtime.
      declare on: (name: string, cb?: any) => unknown;

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
    myObject.on("info", (...args: any[]) => {
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
      // explicitly release it. Otherwise a `slowPromise, 50` job 1 could finish
      // before all 4 remaining jobs are enqueued (each enqueue barrier adds a
      // Redis RTT) and a queued job dispatches, making `queued()` count race
      // the minTime gate.
      const hold1 = deferred();

      expect(await limiter.check()).toEqual(true);

      expect(limiter.queued()).toEqual(0);
      expect(await limiter.clusterQueued()).toEqual(0);

      const p1 = limiter.schedule({ id: 1 as any }, h.deferredPromise, hold1.signal, null, 1);
      await enqueued(limiter);
      expect(limiter.queued()).toEqual(0); // It's already running

      expect(await limiter.check()).toEqual(false);

      const p2 = limiter.schedule({ id: 2 as any }, h.slowPromise, 50, null, 2);
      await enqueued(limiter);
      expect(limiter.queued()).toEqual(1);
      expect(await limiter.clusterQueued()).toEqual(1);
      expect(limiter.queued(1)).toEqual(0);
      expect(limiter.queued(5)).toEqual(1);

      const p3 = limiter.schedule({ id: 3 as any }, h.slowPromise, 50, null, 3);
      await enqueued(limiter);
      expect(limiter.queued()).toEqual(2);
      expect(await limiter.clusterQueued()).toEqual(2);
      expect(limiter.queued(1)).toEqual(0);
      expect(limiter.queued(5)).toEqual(2);

      const p4 = limiter.schedule({ id: 4 as any }, h.slowPromise, 50, null, 4);
      await enqueued(limiter);
      expect(limiter.queued()).toEqual(3);
      expect(await limiter.clusterQueued()).toEqual(3);
      expect(limiter.queued(1)).toEqual(0);
      expect(limiter.queued(5)).toEqual(3);

      const p5 = limiter.schedule({ priority: 1, id: 5 as any }, h.promise, null, 5);
      await enqueued(limiter);
      expect(limiter.queued()).toEqual(4);
      expect(await limiter.clusterQueued()).toEqual(4);
      expect(limiter.queued(1)).toEqual(1);
      expect(limiter.queued(5)).toEqual(3);

      hold1.release();

      await h.flushLimiter(limiter);
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
        expect(p4).resolves.toEqual([4]),
        expect(p5).resolves.toEqual([5]),
      ]);
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

      const p1 = limiter.schedule(
        { weight: 1, id: 1 as any },
        h.deferredPromise,
        hold1.signal,
        null,
        1,
      );
      const p2 = limiter.schedule(
        { weight: 3, id: 2 as any },
        h.deferredPromise,
        hold2.signal,
        null,
        2,
      );
      const p3 = limiter.schedule(
        { weight: 1, id: 3 as any },
        h.deferredPromise,
        hold3.signal,
        null,
        3,
      );
      await limiter.schedule({ weight: 0, id: 4 as any }, h.promise, null);

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
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
      ]);
      expect(h.log).toHaveCallOrder([[], [1], [3], [2]]);
    });

    test("Should refuse duplicate Job IDs", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2, minTime: 100, trackDoneStatus: true });

      try {
        await limiter.schedule({ id: "a" }, h.promise, null, 1);
        await limiter.schedule({ id: "b" }, h.promise, null, 2);
        await limiter.schedule({ id: "a" }, h.promise, null, 3);
      } catch (e) {
        expect((e as Error).message).toEqual("A job with the same id already exists (id=a)");
      }
    });

    test("Should return job statuses", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 2, minTime: 100 });
      await limiter.ready();

      expect(limiter.counts()).toEqual({ RECEIVED: 0, QUEUED: 0, RUNNING: 0, EXECUTING: 0 });

      // RUNNING is transient (dispatched, doExecute timer pending) and a
      // real-clock register landing after minTime elapsed skips it entirely
      // — so it is observed synchronously inside job 2's own "scheduled"
      // event (doRun transitions the state BEFORE triggering the event,
      // Job.js), never via a wall-clock poll.
      let job2StatusAtScheduled = null;
      limiter.on("scheduled", (info) => {
        if ((info.options.id as unknown) === 2) job2StatusAtScheduled = limiter.jobStatus(2 as any);
      });

      const hold1 = deferred();
      const p1 = limiter.schedule(
        { weight: 1, id: 1 as any },
        h.deferredPromise,
        hold1.signal,
        null,
        1,
      );
      const p2 = limiter.schedule({ weight: 1, id: 2 as any }, h.slowPromise, 200, null, 2);
      const p3 = limiter.schedule({ weight: 2, id: 3 as any }, h.slowPromise, 100, null, 3);
      expect(limiter.counts()).toEqual({ RECEIVED: 3, QUEUED: 0, RUNNING: 0, EXECUTING: 0 });

      // Stable point: job 1 held-EXECUTING, job 3 capacity-blocked (weight
      // 2 > remaining 1) — persists until release. (Job 2 may be EXECUTING
      // here too under a stalled register, hence the >=.)
      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.EXECUTING).toBeGreaterThanOrEqual(1);
      });
      expect(limiter.jobStatus(1 as any)).toEqual("EXECUTING");
      expect(limiter.jobStatus(3 as any)).toEqual("QUEUED");
      expect(job2StatusAtScheduled).toEqual("RUNNING");

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

      // RUNNING observed event-synchronously — see "Should return job
      // statuses" above.
      let job2StatusAtScheduled = null;
      limiter.on("scheduled", (info) => {
        if ((info.options.id as unknown) === 2) job2StatusAtScheduled = limiter.jobStatus(2 as any);
      });

      const hold1 = deferred();
      const p1 = limiter.schedule(
        { weight: 1, id: 1 as any },
        h.deferredPromise,
        hold1.signal,
        null,
        1,
      );
      const p2 = limiter.schedule({ weight: 1, id: 2 as any }, h.slowPromise, 200, null, 2);
      const p3 = limiter.schedule({ weight: 2, id: 3 as any }, h.slowPromise, 100, null, 3);
      expect(limiter.counts()).toEqual({
        RECEIVED: 3,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      // Stable point: job 1 held-EXECUTING, job 3 capacity-blocked.
      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.RECEIVED).toBe(0);
        expect(counts.QUEUED).toBe(1);
        expect(counts.EXECUTING).toBeGreaterThanOrEqual(1);
        expect(counts.DONE).toBe(0);
      });
      expect(limiter.jobStatus(1 as any)).toEqual("EXECUTING");
      expect(limiter.jobStatus(3 as any)).toEqual("QUEUED");
      expect(job2StatusAtScheduled).toEqual("RUNNING");

      hold1.release();

      // After hold1.release(): job 1 is DONE; job 2 is mid-execution
      // (200ms, dispatch-gated by minTime) so EXECUTING=1 is stable; job 3
      // (weight 2) still cannot dispatch under the remaining capacity of 1.
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
      expect(limiter.jobStatus(1 as any)).toEqual("DONE");
      expect(limiter.jobStatus(2 as any)).toEqual("EXECUTING");
      expect(limiter.jobStatus(3 as any)).toEqual("QUEUED");

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

      // Every job is held by a deferred signal, so every asserted state is
      // stable until a test-controlled release — no polling of wall-clock
      // windows. (The old design polled for a transient RUNNING window
      // whose existence depended on register round-trips beating minTime;
      // a stalled register landed after minTime elapsed, collapsed the
      // window to zero, and timed out the poll under load.) The one
      // genuinely transient state — RUNNING, i.e. dispatched with the
      // doExecute timer pending — is observed synchronously inside job 2's
      // own "scheduled" event: doRun transitions the state BEFORE
      // triggering the event (Job.js), and doExecute is timer-gated and
      // cannot have fired inside the handler.
      const scheduledRunning: { id: any; running: string[] }[] = [];
      limiter.on("scheduled", (info) => {
        scheduledRunning.push({ id: info.options.id, running: limiter.jobs("RUNNING") });
      });

      const hold1 = deferred();
      const hold2 = deferred();
      const hold3 = deferred();

      const p1 = limiter.schedule(
        { weight: 1, id: 1 as any },
        h.deferredPromise,
        hold1.signal,
        null,
        1,
      );
      const p2 = limiter.schedule(
        { weight: 1, id: 2 as any },
        h.deferredPromise,
        hold2.signal,
        null,
        2,
      );
      const p3 = limiter.schedule(
        { weight: 2, id: 3 as any },
        h.deferredPromise,
        hold3.signal,
        null,
        3,
      );
      expect(limiter.counts()).toEqual({
        RECEIVED: 3,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      expect(limiter.jobs()).toEqual(["1", "2", "3"]);
      expect(limiter.jobs("RECEIVED")).toEqual(["1", "2", "3"]);

      // Stable point: both slots held-EXECUTING, job 3 capacity-blocked
      // (weight 2 > 0 remaining). Persists until we release.
      await waitForState(() => {
        expect(limiter.counts()).toEqual({
          RECEIVED: 0,
          QUEUED: 1,
          RUNNING: 0,
          EXECUTING: 2,
          DONE: 0,
        });
      });
      expect(limiter.jobs("EXECUTING")).toEqual(["1", "2"]);
      expect(limiter.jobs("QUEUED")).toEqual(["3"]);

      // Event-synchronous RUNNING proof: at job 2's own "scheduled", its
      // QUEUED→RUNNING transition had already happened. Job 1 may also
      // still be RUNNING here (its doExecute is a 0ms timer that can lose
      // to this synchronous observation), so assert membership only.
      const job2Snapshot = scheduledRunning.find((s) => s.id === 2);
      expect(job2Snapshot).toBeDefined();
      expect(job2Snapshot!.running).toContain("2");

      hold1.release();

      // Job 1 completes; job 3 (weight 2) still cannot dispatch while job 2
      // (held) occupies a slot.
      await waitForState(() => {
        expect(limiter.counts()).toEqual({
          RECEIVED: 0,
          QUEUED: 1,
          RUNNING: 0,
          EXECUTING: 1,
          DONE: 1,
        });
      });
      expect(limiter.jobs("DONE")).toEqual(["1"]);
      expect(limiter.jobs("EXECUTING")).toEqual(["2"]);
      expect(limiter.jobs("QUEUED")).toEqual(["3"]);

      hold2.release();

      // Job 2 completes; capacity 2 frees; job 3 dispatches and is held.
      await waitForState(() => {
        expect(limiter.counts()).toEqual({
          RECEIVED: 0,
          QUEUED: 0,
          RUNNING: 0,
          EXECUTING: 1,
          DONE: 2,
        });
      });
      expect(limiter.jobs("EXECUTING")).toEqual(["3"]);

      hold3.release();
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
      ]);
      await h.flushLimiter(limiter);

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

      // All three jobs held by deferred signals — event counts are asserted
      // only at stable points that persist until a test-controlled release
      // (see "Should return jobs for a status" for the window-polling
      // rationale this replaces).
      const hold1 = deferred();
      const hold2 = deferred();
      const hold3 = deferred();
      const p1 = limiter.schedule(
        { weight: 1, id: 1 as any },
        h.deferredPromise,
        hold1.signal,
        null,
        1,
      );
      const p2 = limiter.schedule(
        { weight: 1, id: 2 as any },
        h.deferredPromise,
        hold2.signal,
        null,
        2,
      );
      const p3 = limiter.schedule(
        { weight: 2, id: 3 as any },
        h.deferredPromise,
        hold3.signal,
        null,
        3,
      );
      expect(limiter.counts()).toEqual({
        RECEIVED: 3,
        QUEUED: 0,
        RUNNING: 0,
        EXECUTING: 0,
        DONE: 0,
      });

      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([3, 0, 0, 0, 0]);

      await waitForState(() => {
        expect(limiter.counts()).toEqual({
          RECEIVED: 0,
          QUEUED: 1,
          RUNNING: 0,
          EXECUTING: 2,
          DONE: 0,
        });
      });
      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([3, 3, 2, 2, 0]);

      hold1.release();

      await waitForState(() => {
        expect(limiter.counts()).toEqual({
          RECEIVED: 0,
          QUEUED: 1,
          RUNNING: 0,
          EXECUTING: 1,
          DONE: 1,
        });
      });
      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([3, 3, 2, 2, 1]);

      hold2.release();

      await waitForState(() => {
        expect(limiter.counts()).toEqual({
          RECEIVED: 0,
          QUEUED: 0,
          RUNNING: 0,
          EXECUTING: 1,
          DONE: 2,
        });
      });
      expect([onReceived, onQueued, onScheduled, onExecuting, onDone]).toEqual([3, 3, 3, 3, 2]);

      hold3.release();
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
      ]);
      await h.flushLimiter(limiter);

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

      const returned = limiter.on("ready", () => {}) as BottleneckBase;
      // The contract is that `.on()` returns the limiter itself for chaining;
      // compare to `limiter.id` rather than the literal "test-limiter" so this
      // works in Redis projects where test/bottleneck.mjs prefixes ids per fork.
      expect(returned.id).toEqual(limiter.id);
    });

    test("Should fire events on empty queue", async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });
      let calledEmpty = 0;
      let calledIdle = 0;
      let calledDepleted = 0;
      const thirdEmpty = deferred();

      limiter.on("empty", () => {
        calledEmpty++;
        if (calledEmpty === 3) {
          thirdEmpty.release();
        }
      });
      limiter.on("idle", () => {
        calledIdle++;
      });
      limiter.on("depleted", () => {
        calledDepleted++;
      });

      await expect(limiter.schedule({ id: 1 as any }, h.slowPromise, 50, null, 1)).resolves.toEqual(
        [1],
      );
      expect(calledEmpty).toEqual(1);
      expect(calledIdle).toEqual(1);
      await Promise.all([
        expect(limiter.schedule({ id: 2 as any }, h.slowPromise, 50, null, 2)).resolves.toEqual([
          2,
        ]),
        expect(limiter.schedule({ id: 3 as any }, h.slowPromise, 50, null, 3)).resolves.toEqual([
          3,
        ]),
      ]);
      // Fire job 4 and wait for its enqueue to trigger the third "empty" —
      // the counters below must be observed while job 4 is still pending.
      // An enqueued() barrier cannot be used here: the empty() check requires
      // the submit lock to be idle, so a pending barrier task would
      // suppress the very event under test.
      const p4 = limiter.schedule({ id: 4 as any }, h.slowPromise, 50, null, 4);
      await thirdEmpty.signal;
      expect(h).toHaveFinalCallAt(250);
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
      expect(calledEmpty).toEqual(3);
      expect(calledIdle).toEqual(2);
      expect(calledDepleted).toEqual(0);
      await h.flushLimiter(limiter);
      await expect(p4).resolves.toEqual([4]);
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
      const errored = new Promise<void>((resolve) => {
        limiter.on("error", (err) => {
          if ((err as Error).message === "Oh noes!" && !fired) {
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
      const errored = new Promise<void>((resolve) => {
        limiter.on("error", (err) => {
          if ((err as Error).message === "It broke!" && !fired) {
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

  describe("Datastore errors", () => {
    test("Should refuse an unknown datastore type", () => {
      expect(() => new Bottleneck({ datastore: "carrier-pigeon" })).toThrow(
        "Invalid datastore type: carrier-pigeon",
      );
    });

    test("Should swallow DisconnectsClientError while disconnecting", async ({
      harness: h,
      makeLimiter,
    }) => {
      const limiter = makeLimiter({ maxConcurrent: 1 });
      const errors: any[] = [];
      limiter.on("error", (e) => errors.push(e));

      limiter._store._disconnecting = true;
      limiter._store.__free__ = async () => {
        throw disconnectError();
      };

      await expect(limiter.schedule({ id: "swallowed" }, h.promise, null, 1)).resolves.toEqual([1]);
      expect(errors).toEqual([]);
    });

    test("Should surface DisconnectsClientError while not disconnecting", async ({
      harness: h,
      makeLimiter,
    }) => {
      // Fires an expected "error" event; expectErrors silences the harness watchdog log for it.
      const limiter = makeLimiter({ maxConcurrent: 1 }, { expectErrors: true });
      const errored = deferred();
      limiter.on("error", (e) => {
        if ((e as Error).message === "connection is closed") errored.release();
      });

      limiter._store.__free__ = async () => {
        throw disconnectError();
      };

      await Promise.all([
        expect(limiter.schedule({ id: "surfaced" }, h.promise, null, 1)).resolves.toEqual([1]),
        errored,
      ]);
    });

    test("Should swallow disconnect errors raised while draining the queue", async ({
      makeLimiter,
    }) => {
      const limiter = makeLimiter({ maxConcurrent: 1 });
      const errors: any[] = [];
      limiter.on("error", (e) => errors.push(e));

      limiter._store._disconnecting = true;
      limiter._drainOne = async () => {
        throw disconnectError();
      };

      await limiter._drainAll(1);
      expect(errors).toEqual([]);
    });

    test("Should surface drain errors while not disconnecting", async ({ makeLimiter }) => {
      // Fires an expected "error" event; expectErrors silences the harness watchdog log for it.
      const limiter = makeLimiter({ maxConcurrent: 1 }, { expectErrors: true });
      const errors: any[] = [];
      limiter.on("error", (e) => errors.push(e));

      limiter._drainOne = async () => {
        throw new Error("drain exploded");
      };

      await limiter._drainAll(1);
      expect(errors.length).toBe(1);
      expect(errors[0]!.message).toBe("drain exploded");
    });
  });

  describe("LocalDatastore", () => {
    test("computePenalty honors an explicit penalty", () => {
      const limiter = new Bottleneck({ penalty: 123 });
      expect((limiter._store as any).computePenalty()).toBe(123);
    });

    test("computePenalty falls back to 5000 ms when minTime is 0", () => {
      const limiter = new Bottleneck({ minTime: 0 });
      expect((limiter._store as any).computePenalty()).toBe(5000);
    });

    test("Restarting the heartbeat clears the previous interval", () => {
      const limiter = new Bottleneck({
        reservoirRefreshInterval: 100,
        reservoirRefreshAmount: 5,
      });
      const { heartbeat } = limiter._store;
      expect(heartbeat).toBeTruthy();

      limiter.updateSettings({ minTime: 100 });
      limiter.updateSettings({ minTime: 200 });

      expect(limiter._store.heartbeat).toBeTruthy();
    });
  });
});
