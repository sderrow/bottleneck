import { describe, it, afterEach, expect } from "vitest";
import { createJobHarness } from "./helpers/job-tracking.js";
import { waitForState } from "./helpers/wait-for-state.js";
const makeLimiter = require("./helpers/limiter");
const Bottleneck = require("./bottleneck");

describe("Promises", function () {
  let limiter;

  afterEach(function () {
    return limiter.disconnect(false);
  });

  it("Should support promises", function () {
    const h = createJobHarness();
    limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    limiter.submit(h.job, null, 1, 9, h.noErrVal(1, 9));
    limiter.submit(h.job, null, 2, h.noErrVal(2));
    limiter.submit(h.job, null, 3, h.noErrVal(3));
    h.pNoErrVal(limiter.schedule(h.promise, null, 4, 5), 4, 5);

    return h.flushLimiter(limiter).then(function (_results) {
      h.checkResultsOrder([[1, 9], [2], [3], [4, 5]]);
      h.checkDuration(300);
    });
  });

  it("Should pass error on failure", function () {
    const h = createJobHarness();
    const failureMessage = "failed";
    limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    return limiter.schedule(h.promise, new Error(failureMessage)).catch(function (err) {
      expect(err.message).toEqual(failureMessage);
    });
  });

  it("Should allow non-Promise returns", function () {
    limiter = makeLimiter();
    const str = "This is a string";

    return limiter
      .schedule(() => str)
      .then(function (x) {
        expect(x).toEqual(str);
      });
  });

  it("Should get rejected when rejectOnDrop is true", function () {
    const h = createJobHarness();
    limiter = makeLimiter({
      maxConcurrent: 1,
      minTime: 0,
      highWater: 1,
      strategy: Bottleneck.strategy.OVERFLOW,
      rejectOnDrop: true,
    });
    let dropped = 0;
    let caught = 0;
    let p1;
    let p2;

    limiter.on("dropped", function () {
      dropped++;
    });

    p1 = h.pNoErrVal(limiter.schedule({ id: 1 }, h.slowPromise, 50, null, 1), 1);
    p2 = h.pNoErrVal(limiter.schedule({ id: 2 }, h.slowPromise, 50, null, 2), 2);

    return limiter
      .schedule({ id: 3 }, h.slowPromise, 50, null, 3)
      .catch(function (err) {
        expect(err.message).toEqual("This job has been dropped by Bottleneck");
        expect(err).toBeInstanceOf(Bottleneck.BottleneckError);
        caught++;
        return Promise.all([p1, p2]);
      })
      .then(function () {
        return h.flushLimiter(limiter);
      })
      .then(function (_results) {
        h.checkResultsOrder([[1], [2]]);
        h.checkDuration(100);
        expect(dropped).toEqual(1);
        expect(caught).toEqual(1);
      });
  });

  it("Should automatically wrap an exception in a rejected promise - schedule()", function () {
    limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    return limiter
      .schedule(() => {
        throw new Error("I will reject");
      })
      .then(() => expect.fail("should not resolve"))
      .catch((err) => {
        expect(err.message).toBe("I will reject");
      });
  });

  describe("Wrap", function () {
    let fn;
    it("Should wrap", function () {
      const h = createJobHarness();
      limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

      limiter.submit(h.job, null, 1, h.noErrVal(1));
      limiter.submit(h.job, null, 2, h.noErrVal(2));
      limiter.submit(h.job, null, 3, h.noErrVal(3));

      const wrapped = limiter.wrap(h.promise);
      h.pNoErrVal(wrapped(null, 4), 4);

      return h.flushLimiter(limiter).then(function (_results) {
        h.checkResultsOrder([[1], [2], [3], [4]]);
        h.checkDuration(300);
      });
    });

    it("Should automatically wrap a returned value in a resolved promise", function () {
      limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

      fn = limiter.wrap(() => {
        return 7;
      });

      return fn().then((result) => {
        expect(result).toEqual(7);
      });
    });

    it("Should automatically wrap an exception in a rejected promise", function () {
      limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

      fn = limiter.wrap(() => {
        throw new Error("I will reject");
      });

      return fn()
        .then(() => expect.fail("should not resolve"))
        .catch((error) => {
          expect(error.message).toBe("I will reject");
        });
    });

    it("Should inherit the original target for wrapped methods", function () {
      limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

      const object = {
        fn: limiter.wrap(function () {
          return this;
        }),
      };

      return object.fn().then((result) => {
        expect(result).toEqual(object);
      });
    });

    it("Should inherit the original target on prototype methods", function () {
      limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

      class Animal {
        constructor(name) {
          this.name = name;
        }
        getName() {
          return this.name;
        }
      }

      Animal.prototype.getName = limiter.wrap(Animal.prototype.getName);
      let elephant = new Animal("Dumbo");

      return elephant.getName().then((result) => {
        expect(result).toEqual("Dumbo");
      });
    });

    it("Should pass errors back", function () {
      const failureMessage = "BLEW UP!!!";
      const h = createJobHarness();
      limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

      const wrapped = limiter.wrap(h.promise);
      h.pNoErrVal(wrapped(null, 1), 1);
      h.pNoErrVal(wrapped(null, 2), 2);

      return wrapped(new Error(failureMessage), 3)
        .catch(function (err) {
          expect(err.message).toEqual(failureMessage);
          return h.flushLimiter(limiter);
        })
        .then(function (_results) {
          h.checkResultsOrder([[1], [2], [3]]);
          h.checkDuration(200);
        });
    });

    it("Should allow passing options", async function () {
      const failureMessage = "BLEW UP!!!";
      const h = createJobHarness();
      limiter = makeLimiter({ maxConcurrent: 1, minTime: 50 });

      let releasePrimer;
      const primerHeld = new Promise(function (r) {
        releasePrimer = r;
      });
      limiter.schedule(function () {
        return primerHeld;
      });

      const wrapped = limiter.wrap(h.promise);
      h.pNoErrVal(wrapped(null, 1), 1);
      h.pNoErrVal(wrapped(null, 2), 2);
      h.pNoErrVal(wrapped(null, 3), 3);
      h.pNoErrVal(wrapped(null, 4), 4);
      h.pNoErrVal(wrapped.withOptions({ priority: 1 }, null, 5), 5);
      const job6 = wrapped.withOptions({ priority: 1 }, new Error(failureMessage), 6);

      await waitForState(() => {
        expect(limiter.queued()).toBe(6);
      });
      releasePrimer();

      return job6
        .catch(function (err) {
          expect(err.message).toEqual(failureMessage);
          return h.flushLimiter(limiter);
        })
        .then(function (_results) {
          h.checkResultsOrder([[5], [6], [1], [2], [3], [4]]);
        });
    });
  });
});
