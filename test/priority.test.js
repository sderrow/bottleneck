import { useFakeClock, isFakeClock } from "./helpers/clock.js";
import { test, describe, expect, waitForState, deferred } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

useFakeClock();

describe("Priority", () => {
  test("Should do basic ordering", function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100, rejectOnDrop: false });

    return Promise.all([
      h.pNoErrVal(limiter.schedule(h.slowPromise, 50, null, 1), 1),
      h.pNoErrVal(limiter.schedule(h.promise, null, 2), 2),
      h.pNoErrVal(limiter.schedule({ priority: 1 }, h.promise, null, 5, 6), 5, 6),
      h.pNoErrVal(limiter.schedule(h.promise, null, 3), 3),
      h.pNoErrVal(limiter.schedule(h.promise, null, 4), 4),
    ])
      .then(function () {
        return h.flushLimiter(limiter);
      })
      .then(function (_results) {
        expect(h.log).toHaveCallOrder([[1], [5, 6], [2], [3], [4]]);
        expect(h).toHaveFinalCallAt(400);
      });
  });

  test("Should support LEAK", async function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 3,
      strategy: Bottleneck.strategy.LEAK,
      rejectOnDrop: false,
    });

    let called = false;
    limiter.on("dropped", function (dropped) {
      expect(dropped.task).toBeTruthy();
      expect(dropped.args).toBeTruthy();
      expect(dropped.promise).toBeTruthy();
      called = true;
    });

    const first = deferred();

    const subs = [
      limiter.submit(h.deferredJob, first.signal, null, 1, h.noErrVal(1)),
      limiter.submit(h.job, null, 2, h.noErrVal(2)),
      limiter.submit(h.job, null, 3, h.noErrVal(3)),
      limiter.submit(h.job, null, 4, h.noErrVal(4)),
      limiter.submit({ priority: 2 }, h.job, null, 5, h.noErrVal(5)),
      limiter.submit({ priority: 1 }, h.job, null, 6, h.noErrVal(6)),
      limiter.submit({ priority: 9 }, h.job, null, 7, h.noErrVal(7)),
    ];
    await Promise.all(subs);
    first.release();

    return h.flushLimiter(limiter, { weight: 0 }).then(function (_results) {
      expect(h.log).toHaveCallOrder([[1], [6], [5]]);
      expect(called).toEqual(true);
    });
  });

  test("Should support OVERFLOW", async function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 2,
      strategy: Bottleneck.strategy.OVERFLOW,
      rejectOnDrop: false,
    });
    let called = false;
    limiter.on("dropped", function (dropped) {
      expect(dropped.task).toBeTruthy();
      expect(dropped.args).toBeTruthy();
      expect(dropped.promise).toBeTruthy();
      called = true;
    });

    const first = deferred();

    const subs = [
      limiter.submit(h.deferredJob, first.signal, null, 1, h.noErrVal(1)),
      limiter.submit(h.job, null, 2, h.noErrVal(2)),
      limiter.submit(h.job, null, 3, h.noErrVal(3)),
      limiter.submit(h.job, null, 4, h.noErrVal(4)),
      limiter.submit({ priority: 2 }, h.job, null, 5, h.noErrVal(5)),
      limiter.submit({ priority: 1 }, h.job, null, 6, h.noErrVal(6)),
      limiter.submit({ priority: 9 }, h.job, null, 7, h.noErrVal(7)),
    ];
    await Promise.all(subs);
    first.release();

    return limiter
      .updateSettings({ highWater: null })
      .then(function () {
        return h.flushLimiter(limiter);
      })
      .then(function (_results) {
        expect(h.log).toHaveCallOrder([[1], [2], [3]]);
        expect(called).toEqual(true);
      });
  });

  test("Should support OVERFLOW_PRIORITY", async function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 2,
      strategy: Bottleneck.strategy.OVERFLOW_PRIORITY,
      rejectOnDrop: false,
    });
    let called = false;
    limiter.on("dropped", function (dropped) {
      expect(dropped.task).toBeTruthy();
      expect(dropped.args).toBeTruthy();
      expect(dropped.promise).toBeTruthy();
      called = true;
    });

    const first = deferred();

    const subs = [
      limiter.submit(h.deferredJob, first.signal, null, 1, h.noErrVal(1)),
      limiter.submit(h.job, null, 2, h.noErrVal(2)),
      limiter.submit(h.job, null, 3, h.noErrVal(3)),
      limiter.submit(h.job, null, 4, h.noErrVal(4)),
      limiter.submit({ priority: 2 }, h.job, null, 5, h.noErrVal(5)),
      limiter.submit({ priority: 2 }, h.job, null, 6, h.noErrVal(6)),
      limiter.submit({ priority: 2 }, h.job, null, 7, h.noErrVal(7)),
    ];
    await Promise.all(subs);
    first.release();

    return limiter
      .updateSettings({ highWater: null })
      .then(function () {
        return h.flushLimiter(limiter);
      })
      .then(function (_results) {
        expect(h.log).toHaveCallOrder([[1], [5], [6]]);
        expect(called).toEqual(true);
      });
  });

  test("Should support BLOCK", function ({ harness: h, makeLimiter }) {
    expect.hasAssertions();
    const limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 100,
      highWater: 2,
      trackDoneStatus: true,
      strategy: Bottleneck.strategy.BLOCK,
    });
    let called = 0;

    return new Promise(function (resolve) {
      const first = deferred();

      limiter.on("dropped", function (dropped) {
        expect(dropped.task).toBeTruthy();
        expect(dropped.args).toBeTruthy();
        expect(dropped.promise).toBeTruthy();
        called++;
        if (called === 3) {
          limiter
            .updateSettings({ highWater: null })
            .then(function () {
              return limiter.schedule(h.job, null, 8);
            })
            .catch(function (err) {
              expect(err).toBeInstanceOf(Bottleneck.BottleneckError);
              expect(err.message).toEqual("This job has been dropped by Bottleneck");
              limiter.removeAllListeners("error");
              first.release();
              resolve();
            });
        }
      });

      limiter.submit(h.deferredJob, first.signal, null, 1, h.noErrVal(1));
      limiter.submit(h.slowJob, 20, null, 2, (err) => expect(err).toBeTruthy());
      limiter.submit(h.slowJob, 20, null, 3, (err) => expect(err).toBeTruthy());
      limiter.submit(h.slowJob, 20, null, 4, (err) => expect(err).toBeTruthy());
    });
  });

  test("Should have the right priority", async function ({ harness: h, makeLimiter }) {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    let committed = 0;
    limiter.on("queued", function () {
      committed++;
    });
    const first = deferred();
    h.pNoErrVal(limiter.schedule({ priority: 6 }, h.deferredPromise, first.signal, null, 1), 1);
    h.pNoErrVal(limiter.schedule({ priority: 5 }, h.promise, null, 2), 2);
    h.pNoErrVal(limiter.schedule({ priority: 4 }, h.promise, null, 3), 3);
    h.pNoErrVal(limiter.schedule({ priority: 3 }, h.promise, null, 4), 4);
    await waitForState(() => {
      expect(committed).toBe(4);
    });
    first.release();

    return h.flushLimiter(limiter).then(function (_results) {
      if (isFakeClock()) {
        expect(h.results().elapsed).toBe(400);
      } else {
        expect(h.results().elapsed).toBeGreaterThanOrEqual(295);
      }
      expect(h.log).toHaveCallOrder([[1], [4], [3], [2]]);
    });
  });
});
