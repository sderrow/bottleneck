import { useFakeClock } from "./helpers/clock.js";
import { test, describe, expect, waitForState, deferred } from "./helpers/test-api.js";
const Bottleneck = require("./bottleneck");

useFakeClock();

describe("Promises", () => {
  test("Should support promises", ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    limiter.submit(h.job, null, 1, 9, h.noErrVal(1, 9));
    limiter.submit(h.job, null, 2, h.noErrVal(2));
    limiter.submit(h.job, null, 3, h.noErrVal(3));
    h.pNoErrVal(limiter.schedule(h.promise, null, 4, 5), 4, 5);

    return h.flushLimiter(limiter).then((_results) => {
      expect(h.log).toHaveCallOrder([[1, 9], [2], [3], [4, 5]]);
      expect(h).toHaveFinalCallAt(300);
    });
  });

  test("Should pass error on failure", ({ harness: h, makeLimiter }) => {
    const failureMessage = "failed";
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    return limiter.schedule(h.promise, new Error(failureMessage)).catch((err) => {
      expect(err.message).toEqual(failureMessage);
    });
  });

  test("Should allow non-Promise returns", ({ makeLimiter }) => {
    const limiter = makeLimiter();
    const str = "This is a string";

    return limiter
      .schedule(() => str)
      .then((x) => {
        expect(x).toEqual(str);
      });
  });

  test("Should get rejected when rejectOnDrop is true", ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({
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

    limiter.on("dropped", () => {
      dropped++;
    });

    p1 = h.pNoErrVal(limiter.schedule({ id: 1 }, h.slowPromise, 50, null, 1), 1);
    p2 = h.pNoErrVal(limiter.schedule({ id: 2 }, h.slowPromise, 50, null, 2), 2);

    return limiter
      .schedule({ id: 3 }, h.slowPromise, 50, null, 3)
      .catch((err) => {
        expect(err.message).toEqual("This job has been dropped by Bottleneck");
        expect(err).toBeInstanceOf(Bottleneck.BottleneckError);
        caught++;
        return Promise.all([p1, p2]);
      })
      .then(() => h.flushLimiter(limiter))
      .then((_results) => {
        expect(h.log).toHaveCallOrder([[1], [2]]);
        expect(h).toHaveFinalCallAt(100);
        expect(dropped).toEqual(1);
        expect(caught).toEqual(1);
      });
  });

  test("Should automatically wrap an exception in a rejected promise - schedule()", ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    return limiter
      .schedule(() => {
        throw new Error("I will reject");
      })
      .then(() => expect.fail("should not resolve"))
      .catch((err) => {
        expect(err.message).toBe("I will reject");
      });
  });

  describe("Wrap", () => {
    let fn;
    test.override({ limiterOptions: { maxConcurrent: 1, minTime: 100 } });

    test("Should wrap", ({ harness: h, limiter }) => {
      limiter.submit(h.job, null, 1, h.noErrVal(1));
      limiter.submit(h.job, null, 2, h.noErrVal(2));
      limiter.submit(h.job, null, 3, h.noErrVal(3));

      const wrapped = limiter.wrap(h.promise);
      h.pNoErrVal(wrapped(null, 4), 4);

      return h.flushLimiter(limiter).then((_results) => {
        expect(h.log).toHaveCallOrder([[1], [2], [3], [4]]);
        expect(h).toHaveFinalCallAt(300);
      });
    });

    test("Should automatically wrap a returned value in a resolved promise", ({ limiter }) => {
      fn = limiter.wrap(() => 7);

      return fn().then((result) => {
        expect(result).toEqual(7);
      });
    });

    test("Should automatically wrap an exception in a rejected promise", ({ limiter }) => {
      fn = limiter.wrap(() => {
        throw new Error("I will reject");
      });

      return fn()
        .then(() => expect.fail("should not resolve"))
        .catch((error) => {
          expect(error.message).toBe("I will reject");
        });
    });

    test("Should inherit the original target for wrapped methods", ({ limiter }) => {
      const object = {
        fn: limiter.wrap(function () {
          return this;
        }),
      };

      return object.fn().then((result) => {
        expect(result).toEqual(object);
      });
    });

    test("Should inherit the original target on prototype methods", ({ limiter }) => {
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

    test("Should pass errors back", ({ harness: h, limiter }) => {
      const failureMessage = "BLEW UP!!!";

      const wrapped = limiter.wrap(h.promise);
      h.pNoErrVal(wrapped(null, 1), 1);
      h.pNoErrVal(wrapped(null, 2), 2);

      return wrapped(new Error(failureMessage), 3)
        .catch((err) => {
          expect(err.message).toEqual(failureMessage);
          return h.flushLimiter(limiter);
        })
        .then((_results) => {
          expect(h.log).toHaveCallOrder([[1], [2], [3]]);
          expect(h).toHaveFinalCallAt(200);
        });
    });

    test("Should allow passing options", async ({ harness: h, makeLimiter }) => {
      const failureMessage = "BLEW UP!!!";
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 50 });

      const primer = deferred();
      limiter.schedule(() => primer.signal);

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
      primer.release();

      return job6
        .catch((err) => {
          expect(err.message).toEqual(failureMessage);
          return h.flushLimiter(limiter);
        })
        .then((_results) => {
          expect(h.log).toHaveCallOrder([[5], [6], [1], [2], [3], [4]]);
        });
    });
  });
});
