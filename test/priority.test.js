import { describe, expect } from "vitest";
import { useFakeClock, isFakeClock } from "./helpers/clock.js";
import { test, waitForState, deferred, enqueued } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

useFakeClock();

describe("Priority", () => {
  test("Should do basic ordering", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100, rejectOnDrop: false });

    await Promise.all([
      expect(limiter.schedule(h.slowPromise, 50, null, 1)).resolves.toEqual([1]),
      expect(limiter.schedule(h.promise, null, 2)).resolves.toEqual([2]),
      expect(limiter.schedule({ priority: 1 }, h.promise, null, 5, 6)).resolves.toEqual([5, 6]),
      expect(limiter.schedule(h.promise, null, 3)).resolves.toEqual([3]),
      expect(limiter.schedule(h.promise, null, 4)).resolves.toEqual([4]),
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
    // because its own priority (9) is the lowest; 4 is displaced later by the
    // flush job, which is submitted while the queue is still at highWater.)
    limiter.schedule(h.promise, null, 2);
    limiter.schedule(h.promise, null, 3);
    limiter.schedule(h.promise, null, 4);
    const p5 = limiter.schedule({ priority: 2 }, h.promise, null, 5);
    const p6 = limiter.schedule({ priority: 1 }, h.promise, null, 6);
    limiter.schedule({ priority: 9 }, h.promise, null, 7);
    // Enqueue barrier: schedule() resolves at completion, not enqueue, so run
    // a no-op through the submit lock to guarantee all seven submissions
    // above have been processed before releasing.
    await enqueued(limiter);
    first.release();

    await Promise.all([
      expect(p1).resolves.toEqual([1]),
      expect(p5).resolves.toEqual([5]),
      expect(p6).resolves.toEqual([6]),
      h.flushLimiter(limiter, { weight: 0 }),
    ]);
    expect(h.log).toHaveCallOrder([[1], [6], [5]]);
    expect(called).toEqual(true);
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
