import { describe, expect } from "vitest";
import { useFakeClock, isFakeClock } from "./helpers/clock.js";
import { test, waitForState, deferred, enqueued } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

useFakeClock();

describe("Priority", () => {
  test("Should do basic ordering", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100, rejectOnDrop: false });

    // Hold job 1 open with a deferred signal and barrier on all five
    // submissions before releasing. The dispatch triggered by job 1's
    // completion picks from the local queue, so jobs 2-5 must be committed
    // before the first capacity event. With the old real 50ms slowPromise, a
    // slow submit round-trip under parallel-load could lose the race against
    // job 1's completion, letting job 2 dispatch before [5, 6].
    const first = deferred();
    const p1 = limiter.schedule(h.deferredPromise, first.signal, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);
    const p3 = limiter.schedule({ priority: 1 }, h.promise, null, 5, 6);
    const p4 = limiter.schedule(h.promise, null, 3);
    const p5 = limiter.schedule(h.promise, null, 4);
    await enqueued(limiter);
    first.release();

    await Promise.all([
      expect(p1).resolves.toEqual([1]),
      expect(p2).resolves.toEqual([2]),
      expect(p3).resolves.toEqual([5, 6]),
      expect(p4).resolves.toEqual([3]),
      expect(p5).resolves.toEqual([4]),
    ]);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [5, 6], [2], [3], [4]]);
    expect(h).toHaveFinalCallAt(400);
  });

  test("Should support LEAK", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 3,
      strategy: Bottleneck.strategy.LEAK,
      rejectOnDrop: false,
    });

    let called = false;
    limiter.on("dropped", (dropped) => {
      expect(dropped.task).toBeTruthy();
      expect(dropped.args).toBeTruthy();
      expect(dropped.promise).toBeTruthy();
      called = true;
    });

    const first = deferred();

    const p1 = limiter.schedule(h.deferredPromise, first.signal, null, 1);
    // Jobs 2, 3, 4 and 7 are dropped (LEAK, rejectOnDrop: false): their
    // promises never settle, so wrapping them in expect().resolves would hang
    // the test's auto-awaited assertions. toHaveCallOrder below proves they
    // never ran. (2 and 3 are displaced by 5 and 6; 7 is dropped on arrival
    // because its own priority (9) is the lowest; 4 is displaced by the flush
    // job submitted below, while the queue is still at highWater.)
    limiter.schedule(h.promise, null, 2);
    limiter.schedule(h.promise, null, 3);
    limiter.schedule(h.promise, null, 4);
    const p5 = limiter.schedule({ priority: 2 }, h.promise, null, 5);
    const p6 = limiter.schedule({ priority: 1 }, h.promise, null, 6);
    limiter.schedule({ priority: 9 }, h.promise, null, 7);
    // Enqueue barrier: schedule() resolves at completion, not enqueue, so run
    // a no-op through the submit lock to guarantee all seven submissions
    // above have been processed.
    await enqueued(limiter);
    // Displace job 4 deterministically: submit the flush BEFORE releasing job
    // 1, then barrier again, so the flush registers while the queue is still
    // exactly at highWater — job 1 holds the only slot, so nothing can
    // dispatch meanwhile. Submitted after release (the old design), the flush
    // raced job 6's dispatch: under real-clock redis projects, once minTime
    // had elapsed during the submission phase, 6 could drain the queue below
    // highWater before the flush registered, letting 4 survive and run
    // ([[1],[6],[5],[4]]). The default weight (1 — the old design passed an
    // explicit 0) matters: at capacity 0 a weight-0 job passes
    // conditions_check and is admitted without tripping HWM once minTime has
    // elapsed, while weight 1 always fails the check.
    const flush = h.flushLimiter(limiter);
    await enqueued(limiter);
    first.release();

    await Promise.all([
      expect(p1).resolves.toEqual([1]),
      expect(p5).resolves.toEqual([5]),
      expect(p6).resolves.toEqual([6]),
      flush,
    ]);
    expect(h.log).toHaveCallOrder([[1], [6], [5]]);
    expect(called).toEqual(true);
  });

  test("Should drop on LEAK when there is nothing to displace", async ({
    harness: h,
    makeLimiter,
  }) => {
    // highWater 0 with reservoir 0: the very first job trips HWM against an
    // empty queue, so LEAK finds no victim to shift and drops the arriving
    // job itself (shifted == null branch).
    const limiter = makeLimiter({
      highWater: 0,
      reservoir: 0,
      strategy: Bottleneck.strategy.LEAK,
      rejectOnDrop: false,
    });

    let dropped = 0;
    limiter.on("dropped", () => {
      dropped++;
    });

    limiter.schedule(h.promise, null, 1);
    // The arriving job is dropped synchronously inside _addToQueue, before
    // the barrier task runs. (A flush job can't be used as the completion
    // barrier here: with reservoir 0 it trips HWM too and never settles.)
    await enqueued(limiter);

    expect(dropped).toEqual(1);
    expect(h.log).not.toHaveBeenCalled();
  });

  test("Should support OVERFLOW", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 2,
      strategy: Bottleneck.strategy.OVERFLOW,
      rejectOnDrop: false,
    });
    let called = false;
    limiter.on("dropped", (dropped) => {
      expect(dropped.task).toBeTruthy();
      expect(dropped.args).toBeTruthy();
      expect(dropped.promise).toBeTruthy();
      called = true;
    });

    const first = deferred();

    const p1 = limiter.schedule(h.deferredPromise, first.signal, null, 1);
    const p2 = limiter.schedule(h.promise, null, 2);
    const p3 = limiter.schedule(h.promise, null, 3);
    // Jobs 4-7 are dropped on arrival (OVERFLOW, rejectOnDrop: false): their
    // promises never settle, so wrapping them in expect().resolves would hang
    // the test's auto-awaited assertions. toHaveCallOrder below proves they
    // never ran.
    limiter.schedule(h.promise, null, 4);
    limiter.schedule({ priority: 2 }, h.promise, null, 5);
    limiter.schedule({ priority: 1 }, h.promise, null, 6);
    limiter.schedule({ priority: 9 }, h.promise, null, 7);
    // Enqueue barrier: schedule() resolves at completion, not enqueue, so run
    // a no-op through the submit lock to guarantee all seven submissions
    // above have been processed before releasing.
    await enqueued(limiter);
    first.release();

    await limiter.updateSettings({ highWater: null });
    await Promise.all([
      expect(p1).resolves.toEqual([1]),
      expect(p2).resolves.toEqual([2]),
      expect(p3).resolves.toEqual([3]),
      h.flushLimiter(limiter),
    ]);
    expect(h.log).toHaveCallOrder([[1], [2], [3]]);
    expect(called).toEqual(true);
  });

  test("Should support OVERFLOW_PRIORITY", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 2,
      strategy: Bottleneck.strategy.OVERFLOW_PRIORITY,
      rejectOnDrop: false,
    });
    let called = false;
    limiter.on("dropped", (dropped) => {
      expect(dropped.task).toBeTruthy();
      expect(dropped.args).toBeTruthy();
      expect(dropped.promise).toBeTruthy();
      called = true;
    });

    const first = deferred();

    const p1 = limiter.schedule(h.deferredPromise, first.signal, null, 1);
    // Jobs 2, 3, 4 and 7 are dropped (OVERFLOW_PRIORITY, rejectOnDrop: false):
    // their promises never settle, so wrapping them in expect().resolves would
    // hang the test's auto-awaited assertions. toHaveCallOrder below proves
    // they never ran. (2 and 3 are displaced by the higher-priority 5 and 6;
    // 4 and 7 are dropped on arrival with no lower-priority job to displace.)
    limiter.schedule(h.promise, null, 2);
    limiter.schedule(h.promise, null, 3);
    limiter.schedule(h.promise, null, 4);
    const p5 = limiter.schedule({ priority: 2 }, h.promise, null, 5);
    const p6 = limiter.schedule({ priority: 2 }, h.promise, null, 6);
    limiter.schedule({ priority: 2 }, h.promise, null, 7);
    // Enqueue barrier: schedule() resolves at completion, not enqueue, so run
    // a no-op through the submit lock to guarantee all seven submissions
    // above have been processed before releasing.
    await enqueued(limiter);
    first.release();

    await limiter.updateSettings({ highWater: null });
    await Promise.all([
      expect(p1).resolves.toEqual([1]),
      expect(p5).resolves.toEqual([5]),
      expect(p6).resolves.toEqual([6]),
      h.flushLimiter(limiter),
    ]);
    expect(h.log).toHaveCallOrder([[1], [5], [6]]);
    expect(called).toEqual(true);
  });

  test("Should support BLOCK", ({ harness: h, makeLimiter }) => {
    expect.hasAssertions();
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 2,
      trackDoneStatus: true,
      strategy: Bottleneck.strategy.BLOCK,
    });
    let called = 0;

    let p1, p2, p3, p4;
    const unblocked = new Promise((resolve) => {
      const first = deferred();

      limiter.on("dropped", (dropped) => {
        expect(dropped.task).toBeTruthy();
        expect(dropped.args).toBeTruthy();
        expect(dropped.promise).toBeTruthy();
        called++;
        if (called === 3) {
          // Fire-and-forget: the outer Promise only resolves via resolve()
          // in the catch below.
          limiter
            .updateSettings({ highWater: null })
            .then(() => limiter.schedule(h.promise, null, 8))
            .catch((err) => {
              expect(err).toBeInstanceOf(Bottleneck.BottleneckError);
              expect(err.message).toEqual("This job has been dropped by Bottleneck");
              limiter.removeAllListeners("error");
              first.release();
              resolve();
            });
        }
      });

      p1 = limiter.schedule(h.deferredPromise, first.signal, null, 1);
      p2 = limiter.schedule(h.slowPromise, 20, null, 2);
      p3 = limiter.schedule(h.slowPromise, 20, null, 3);
      p4 = limiter.schedule(h.slowPromise, 20, null, 4);
    });

    // Jobs 2-4 are dropped by BLOCK; with the default rejectOnDrop their
    // promises reject with the drop error. Job 1 is already running when the
    // strategy triggers, so it completes once first.release() fires above.
    return Promise.all([
      unblocked,
      expect(p1).resolves.toEqual([1]),
      expect(p2).rejects.toThrow("This job has been dropped by Bottleneck"),
      expect(p3).rejects.toThrow("This job has been dropped by Bottleneck"),
      expect(p4).rejects.toThrow("This job has been dropped by Bottleneck"),
    ]);
  });

  test("Should have the right priority", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    let committed = 0;
    limiter.on("queued", () => {
      committed++;
    });
    const first = deferred();
    const p1 = limiter.schedule({ priority: 6 }, h.deferredPromise, first.signal, null, 1);
    const p2 = limiter.schedule({ priority: 5 }, h.promise, null, 2);
    const p3 = limiter.schedule({ priority: 4 }, h.promise, null, 3);
    const p4 = limiter.schedule({ priority: 3 }, h.promise, null, 4);
    await waitForState(() => {
      expect(committed).toBe(4);
    });
    first.release();

    await h.flushLimiter(limiter);
    await Promise.all([
      expect(p1).resolves.toEqual([1]),
      expect(p2).resolves.toEqual([2]),
      expect(p3).resolves.toEqual([3]),
      expect(p4).resolves.toEqual([4]),
    ]);

    if (isFakeClock()) {
      expect(h.results().elapsed).toBe(400);
    } else {
      expect(h.results().elapsed).toBeGreaterThanOrEqual(295);
    }
    expect(h.log).toHaveCallOrder([[1], [4], [3], [2]]);
  });
});
