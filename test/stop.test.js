import { useFakeClock } from "./helpers/clock.js";
import { test, describe, expect, waitForState } from "./helpers/test-api.js";

useFakeClock();

describe("Stop", () => {
  test("Should stop and drop the queue", async function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({
      maxConcurrent: 2,
      minTime: 100,
      trackDoneStatus: true,
    });
    let dropped = 0;

    limiter.on("dropped", function () {
      dropped++;
    });

    h.pNoErrVal(limiter.schedule({ id: "0" }, h.promise, null, 0), 0);

    h.pNoErrVal(limiter.schedule({ id: "1" }, h.slowPromise, 500, null, 1), 1);

    const scheduledDroppedJob = limiter.schedule({ id: "2" }, h.promise, null, 2);
    const queuedDroppedJob = limiter.schedule({ id: "3" }, h.promise, null, 3);

    await waitForState(() => {
      const counts = limiter.counts();
      expect(counts.RECEIVED).toBe(0);
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
      expect(scheduledDroppedJob).rejects.toThrow("Dropped!"),
      expect(queuedDroppedJob).rejects.toThrow("Dropped!"),
      expect(submitFailedJob).rejects.toThrow("Stopped!"),
    ]);

    const counts = limiter.counts();
    expect(dropped).toEqual(2);
    expect(counts.RECEIVED).toEqual(0);
    expect(counts.QUEUED).toEqual(0);
    expect(counts.RUNNING).toEqual(0);
    expect(counts.EXECUTING).toEqual(0);
    expect(counts.DONE).toEqual(2);

    expect(h.log).toHaveCallOrder([[0], [1]]);
  });

  test("Should stop and let the queue finish", async function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      trackDoneStatus: true,
    });
    let dropped = 0;

    limiter.on("dropped", function () {
      dropped++;
    });

    h.pNoErrVal(limiter.schedule({ id: "1" }, h.promise, null, 1), 1);
    h.pNoErrVal(limiter.schedule({ id: "2" }, h.promise, null, 2), 2);
    h.pNoErrVal(limiter.schedule({ id: "3" }, h.slowPromise, 100, null, 3), 3);

    await waitForState(() => {
      const counts = limiter.counts();
      expect(counts.RECEIVED).toBe(0);
      expect(counts.QUEUED).toBe(1);
      expect(counts.RUNNING).toBe(1);
      expect(counts.EXECUTING).toBe(0);
      expect(counts.DONE).toBe(1);
    });

    const stopPromise = limiter.stop({
      enqueueErrorMessage: "Stopped!",
      dropWaitingJobs: false,
    });
    const submitFailedJob = limiter.schedule(() => Promise.resolve(true));

    await Promise.all([stopPromise, expect(submitFailedJob).rejects.toThrow("Stopped!")]);
    const counts = limiter.counts();
    expect(dropped).toEqual(0);
    expect(counts.RECEIVED).toEqual(0);
    expect(counts.QUEUED).toEqual(0);
    expect(counts.RUNNING).toEqual(0);
    expect(counts.EXECUTING).toEqual(0);
    expect(counts.DONE).toEqual(4);

    expect(h.log).toHaveCallOrder([[1], [2], [3]]);
  });

  test("Should still resolve when rejectOnDrop is false", function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      rejectOnDrop: false,
    });

    h.pNoErrVal(limiter.schedule({ id: "1" }, h.promise, null, 1), 1);
    h.pNoErrVal(limiter.schedule({ id: "2" }, h.promise, null, 2), 2);
    h.pNoErrVal(limiter.schedule({ id: "3" }, h.slowPromise, 100, null, 3), 3);

    return limiter
      .stop()
      .then(function () {
        return limiter.stop();
      })
      .then(function () {
        throw new Error("Should not be here");
      })
      .catch(function (err) {
        expect(err.message).toEqual("stop() has already been called");
      });
  });

  test("Should not allow calling stop() twice when dropWaitingJobs=true", function ({
    harness: h,
    makeLimiter,
  }) {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
    });
    let failed = 0;
    const handler = function (err) {
      expect(err.message).toEqual("This limiter has been stopped.");
      failed++;
    };

    h.pNoErrVal(limiter.schedule({ id: "1" }, h.promise, null, 1), 1).catch(handler);
    h.pNoErrVal(limiter.schedule({ id: "2" }, h.promise, null, 2), 2).catch(handler);
    h.pNoErrVal(limiter.schedule({ id: "3" }, h.slowPromise, 100, null, 3), 3).catch(handler);

    return limiter
      .stop({ dropWaitingJobs: true })
      .then(function () {
        return limiter.stop({ dropWaitingJobs: true });
      })
      .then(function () {
        throw new Error("Should not be here");
      })
      .catch(function (err) {
        expect(err.message).toEqual("stop() has already been called");
        expect(failed).toEqual(3);
      });
  });

  test("Should not allow calling stop() twice when dropWaitingJobs=false", function ({
    harness: h,
    makeLimiter,
  }) {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
    });

    h.pNoErrVal(limiter.schedule({ id: "1" }, h.promise, null, 1), 1);
    h.pNoErrVal(limiter.schedule({ id: "2" }, h.promise, null, 2), 2);
    h.pNoErrVal(limiter.schedule({ id: "3" }, h.slowPromise, 100, null, 3), 3);

    return limiter
      .stop({ dropWaitingJobs: false })
      .then(function () {
        return limiter.stop({ dropWaitingJobs: false });
      })
      .then(function () {
        throw new Error("Should not be here");
      })
      .catch(function (err) {
        expect(err.message).toEqual("stop() has already been called");
      });
  });
});
