import { describe, expect } from "vitest";
import { useFakeClock } from "./helpers/clock";
import { test, waitForState, deferred } from "./helpers/test-api";

useFakeClock();

describe("Stop", () => {
  test("Should stop and drop the queue", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      trackDoneStatus: true,
    });
    let dropped = 0;

    limiter.on("dropped", () => {
      dropped++;
    });

    // Job 1 is held by a deferred signal so EXECUTING=1 is stable, and jobs
    // 2-3 are capacity-blocked so QUEUED=2 is stable — stop() then drops
    // both queued jobs deterministically, with no wall-clock window in the
    // path. (Dropping a scheduled RUNNING job is covered by the
    // fake-clock-only test below: that state is a doExecute-timer window a
    // real-clock poll can miss entirely when a stalled register lands
    // after minTime has already elapsed.)
    const hold1 = deferred();
    const p1 = limiter.schedule({ id: "1" }, h.deferredPromise, hold1.signal, null, 1);
    const p2 = limiter.schedule({ id: "2" }, h.promise, null, 2);
    const p3 = limiter.schedule({ id: "3" }, h.promise, null, 3);

    await waitForState(() => {
      const counts = limiter.counts();
      expect(counts.RECEIVED).toBe(0);
      expect(counts.QUEUED).toBe(2);
      expect(counts.RUNNING).toBe(0);
      expect(counts.EXECUTING).toBe(1);
      expect(counts.DONE).toBe(0);
    });

    const stopPromise = limiter.stop({
      enqueueErrorMessage: "Stopped!",
      dropErrorMessage: "Dropped!",
    });
    const submitFailedJob = limiter.schedule(() => Promise.resolve(true));

    hold1.release();

    await Promise.all([
      stopPromise,
      expect(p1).resolves.toEqual([1]),
      expect(p2).rejects.toThrow("Dropped!"),
      expect(p3).rejects.toThrow("Dropped!"),
      expect(submitFailedJob).rejects.toThrow("Stopped!"),
    ]);

    const counts = limiter.counts();
    expect(dropped).toEqual(2);
    expect(counts.RECEIVED).toEqual(0);
    expect(counts.QUEUED).toEqual(0);
    expect(counts.RUNNING).toEqual(0);
    expect(counts.EXECUTING).toEqual(0);
    expect(counts.DONE).toEqual(1);

    expect(h.log).toHaveCallOrder([[1]]);
  });

  // Dropping a scheduled (RUNNING) job requires catching the dispatched-
  // but-not-executing window — deterministic only under the fake clock,
  // where job 2's dispatch wait is exact. Gated on DATASTORE (the same
  // condition useFakeClock() checks), not isFakeClock(): timers are not
  // installed yet at test-collection time.
  test.runIf(process.env.DATASTORE == null)(
    "Should drop a scheduled RUNNING job on stop",
    async ({ harness: h, makeLimiter }) => {
      const limiter = makeLimiter({
        maxConcurrent: 2,
        minTime: 100,
        trackDoneStatus: true,
      });
      let dropped = 0;
      limiter.on("dropped", () => dropped++);

      const p0 = limiter.schedule({ id: "0" }, h.promise, null, 0);
      const p1 = limiter.schedule({ id: "1" }, h.slowPromise, 500, null, 1);
      const scheduledJob = limiter.schedule({ id: "2" }, h.promise, null, 2);
      const queuedJob = limiter.schedule({ id: "3" }, h.promise, null, 3);

      await waitForState(() => {
        const counts = limiter.counts();
        expect(counts.QUEUED).toBe(1);
        expect(counts.RUNNING).toBe(1);
        expect(counts.EXECUTING).toBe(1);
        expect(counts.DONE).toBe(1);
      });

      const stopPromise = limiter.stop({
        enqueueErrorMessage: "Stopped!",
        dropErrorMessage: "Dropped!",
      });
      const submitFailedJob = limiter.schedule(() => Promise.resolve(true));

      await Promise.all([
        stopPromise,
        expect(p0).resolves.toEqual([0]),
        expect(p1).resolves.toEqual([1]),
        expect(scheduledJob).rejects.toThrow("Dropped!"),
        expect(queuedJob).rejects.toThrow("Dropped!"),
        expect(submitFailedJob).rejects.toThrow("Stopped!"),
      ]);
      expect(dropped).toEqual(2);
      expect(h.log).toHaveCallOrder([[0], [1]]);
    },
  );

  test("Should stop and let the queue finish", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      trackDoneStatus: true,
    });
    let dropped = 0;

    limiter.on("dropped", () => {
      dropped++;
    });

    // Job 1 held; jobs 2-3 queued (stable). stop(dropWaitingJobs=false)
    // keeps them; releasing job 1 drains them in FIFO order.
    const hold1 = deferred();
    const p1 = limiter.schedule({ id: "1" }, h.deferredPromise, hold1.signal, null, 1);
    const p2 = limiter.schedule({ id: "2" }, h.promise, null, 2);
    const p3 = limiter.schedule({ id: "3" }, h.promise, null, 3);

    await waitForState(() => {
      const counts = limiter.counts();
      expect(counts.RECEIVED).toBe(0);
      expect(counts.QUEUED).toBe(2);
      expect(counts.RUNNING).toBe(0);
      expect(counts.EXECUTING).toBe(1);
      expect(counts.DONE).toBe(0);
    });

    const stopPromise = limiter.stop({
      enqueueErrorMessage: "Stopped!",
      dropWaitingJobs: false,
    });
    const submitFailedJob = limiter.schedule(() => Promise.resolve(true));

    hold1.release();

    await Promise.all([
      stopPromise,
      expect(p1).resolves.toEqual([1]),
      expect(p2).resolves.toEqual([2]),
      expect(p3).resolves.toEqual([3]),
      expect(submitFailedJob).rejects.toThrow("Stopped!"),
    ]);
    const counts = limiter.counts();
    expect(dropped).toEqual(0);
    expect(counts.RECEIVED).toEqual(0);
    expect(counts.QUEUED).toEqual(0);
    expect(counts.RUNNING).toEqual(0);
    expect(counts.EXECUTING).toEqual(0);
    expect(counts.DONE).toEqual(4);

    expect(h.log).toHaveCallOrder([[1], [2], [3]]);
  });

  test("Should resolve stop() from the done-event listener", async ({
    harness: h,
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ maxConcurrent: 2 });

    // Two jobs held EXECUTING with nothing queued behind them. The weight-0
    // waitForExecuting job stop() enqueues dispatches immediately (weight 0
    // passes the capacity check), so its initial finished() check fails
    // (counts = 3) and stop can only resolve via the done-event listener
    // once both jobs complete.
    const hold1 = deferred();
    const hold2 = deferred();
    const p1 = limiter.schedule({ id: "1" }, h.deferredPromise, hold1.signal, null, 1);
    const p2 = limiter.schedule({ id: "2" }, h.deferredPromise, hold2.signal, null, 2);

    await waitForState(() => {
      expect(limiter.counts().EXECUTING).toBe(2);
    });

    const stopPromise = limiter.stop({ dropWaitingJobs: false });
    hold1.release();
    hold2.release();

    await Promise.all([
      stopPromise,
      expect(p1).resolves.toEqual([1]),
      expect(p2).resolves.toEqual([2]),
    ]);
    expect(limiter.counts().EXECUTING).toBe(0);
  });

  test("Should still resolve when rejectOnDrop is false", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      rejectOnDrop: false,
    });

    // With rejectOnDrop false, jobs dropped by stop() never settle their
    // promises — so there is deliberately no assertion on them (wrapping in
    // expect().resolves would hang the test's auto-awaited assertions). The
    // contract under test is only that stop() resolves and rejects on reuse.
    limiter.schedule({ id: "1" }, h.promise, null, 1);
    limiter.schedule({ id: "2" }, h.promise, null, 2);
    limiter.schedule({ id: "3" }, h.slowPromise, 100, null, 3);

    await limiter.stop();
    await expect(limiter.stop()).rejects.toThrow("stop() has already been called");
  });

  test("Should not allow calling stop() twice when dropWaitingJobs=true", async ({
    harness: h,
    makeLimiter,
  }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
    });
    // All three jobs are still waiting when stop() drops the queue, so all
    // three promises must reject with the stop message.
    const dropped = [
      limiter.schedule({ id: "1" }, h.promise, null, 1),
      limiter.schedule({ id: "2" }, h.promise, null, 2),
      limiter.schedule({ id: "3" }, h.slowPromise, 100, null, 3),
    ];

    await limiter.stop({ dropWaitingJobs: true });
    await expect(limiter.stop({ dropWaitingJobs: true })).rejects.toThrow(
      "stop() has already been called",
    );
    await Promise.all(
      dropped.map((p) => expect(p).rejects.toThrow("This limiter has been stopped.")),
    );
  });

  test("Should not allow calling stop() twice when dropWaitingJobs=false", async ({
    harness: h,
    makeLimiter,
  }) => {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
    });

    const p1 = limiter.schedule({ id: "1" }, h.promise, null, 1);
    const p2 = limiter.schedule({ id: "2" }, h.promise, null, 2);
    const p3 = limiter.schedule({ id: "3" }, h.slowPromise, 100, null, 3);

    await limiter.stop({ dropWaitingJobs: false });
    await expect(limiter.stop({ dropWaitingJobs: false })).rejects.toThrow(
      "stop() has already been called",
    );
    await Promise.all([
      expect(p1).resolves.toEqual([1]),
      expect(p2).resolves.toEqual([2]),
      expect(p3).resolves.toEqual([3]),
    ]);
  });
});
