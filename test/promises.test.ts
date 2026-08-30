import { describe, expect } from "vitest";
import Bottleneck from "./bottleneck";
import { useFakeClock } from "./helpers/clock";
import { test, waitForState, deferred } from "./helpers/test-api";

useFakeClock();

describe("Promises", () => {
  test("Should support promises", async ({ harness: h, makeLimiter }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    const p1 = limiter.schedule(h.promise, null, 1, 9);
    const p2 = limiter.schedule(h.promise, null, 2);
    const p3 = limiter.schedule(h.promise, null, 3);
    const p4 = limiter.schedule(h.promise, null, 4, 5);

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1, 9], [2], [3], [4, 5]]);
    expect(h).toHaveFinalCallAt(300);
    await Promise.all([
      expect(p1).resolves.toEqual([1, 9]),
      expect(p2).resolves.toEqual([2]),
      expect(p3).resolves.toEqual([3]),
      expect(p4).resolves.toEqual([4, 5]),
    ]);
  });

  test("Should pass error on failure", async ({ harness: h, makeLimiter }) => {
    const failureMessage = "failed";
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    await expect(limiter.schedule(h.promise, new Error(failureMessage))).rejects.toThrow(
      failureMessage,
    );
  });

  test("Should allow non-Promise returns", async ({ makeLimiter }) => {
    const limiter = makeLimiter();
    const str = "This is a string";

    const x = await limiter.schedule(() => str);
    expect(x).toEqual(str);
  });

  test("Should get rejected when rejectOnDrop is true", async ({ harness: h, makeLimiter }) => {
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

    p1 = limiter.schedule({ id: 1 } as any, h.slowPromise, 50, null, 1);
    p2 = limiter.schedule({ id: 2 } as any, h.slowPromise, 50, null, 2);

    try {
      await limiter.schedule({ id: 3 } as any, h.slowPromise, 50, null, 3);
    } catch (err: any) {
      expect(err.message).toEqual("This job has been dropped by Bottleneck");
      expect(err).toBeInstanceOf(Bottleneck.BottleneckError);
      caught++;
      await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
    }

    await h.flushLimiter(limiter);
    expect(h.log).toHaveCallOrder([[1], [2]]);
    expect(h).toHaveFinalCallAt(100);
    expect(dropped).toEqual(1);
    expect(caught).toEqual(1);
  });

  test("Should automatically wrap an exception in a rejected promise - schedule()", async ({
    makeLimiter,
  }) => {
    const limiter = makeLimiter({ maxConcurrent: 1, minTime: 100 });

    await expect(
      limiter.schedule(() => {
        throw new Error("I will reject");
      }),
    ).rejects.toThrow("I will reject");
  });

  describe("Wrap", () => {
    let fn: any;
    test.override({ limiterOptions: { maxConcurrent: 1, minTime: 100 } });

    test("Should wrap", async ({ harness: h, limiter }) => {
      // Wrapped jobs share the same queue as directly scheduled ones.
      const p1 = limiter.schedule(h.promise, null, 1);
      const p2 = limiter.schedule(h.promise, null, 2);
      const p3 = limiter.schedule(h.promise, null, 3);

      const wrapped = limiter.wrap(h.promise);
      const p4 = wrapped(null, 4);

      await h.flushLimiter(limiter);
      expect(h.log).toHaveCallOrder([[1], [2], [3], [4]]);
      expect(h).toHaveFinalCallAt(300);
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
        expect(p4).resolves.toEqual([4]),
      ]);
    });

    test("Should automatically wrap a returned value in a resolved promise", async ({
      limiter,
    }) => {
      fn = limiter.wrap(() => 7);

      const result = await fn();
      expect(result).toEqual(7);
    });

    test("Should automatically wrap an exception in a rejected promise", async ({ limiter }) => {
      fn = limiter.wrap(() => {
        throw new Error("I will reject");
      });

      await expect(fn()).rejects.toThrow("I will reject");
    });

    test("Should inherit the original target for wrapped methods", async ({ limiter }) => {
      const object = {
        fn: limiter.wrap(function (this: unknown) {
          return this;
        }),
      };

      const result = await object.fn();
      expect(result).toEqual(object);
    });

    test("Should inherit the original target on prototype methods", async ({ limiter }) => {
      class Animal {
        name: string;
        constructor(name: string) {
          this.name = name;
        }
        getName() {
          return this.name;
        }
      }

      Animal.prototype.getName = limiter.wrap(Animal.prototype.getName) as any;
      let elephant = new Animal("Dumbo");

      const result = await elephant.getName();
      expect(result).toEqual("Dumbo");
    });

    test("Should pass errors back", async ({ harness: h, limiter }) => {
      const failureMessage = "BLEW UP!!!";

      const wrapped = limiter.wrap(h.promise);
      const p1 = wrapped(null, 1);
      const p2 = wrapped(null, 2);

      await expect(wrapped(new Error(failureMessage), 3)).rejects.toThrow(failureMessage);
      await h.flushLimiter(limiter);
      expect(h.log).toHaveCallOrder([[1], [2], [3]]);
      expect(h).toHaveFinalCallAt(200);
      await Promise.all([expect(p1).resolves.toEqual([1]), expect(p2).resolves.toEqual([2])]);
    });

    test("Should allow passing options", async ({ harness: h, makeLimiter }) => {
      const failureMessage = "BLEW UP!!!";
      const limiter = makeLimiter({ maxConcurrent: 1, minTime: 50 });

      const primer = deferred();
      limiter.schedule(() => primer.signal);

      const wrapped = limiter.wrap(h.promise);
      const p1 = wrapped(null, 1);
      const p2 = wrapped(null, 2);
      const p3 = wrapped(null, 3);
      const p4 = wrapped(null, 4);
      const p5 = wrapped.withOptions({ priority: 1 }, null, 5);
      const job6 = wrapped.withOptions({ priority: 1 }, new Error(failureMessage), 6);

      await waitForState(() => {
        expect(limiter.queued()).toBe(6);
      });
      primer.release();

      await expect(job6).rejects.toThrow(failureMessage);
      await h.flushLimiter(limiter);
      expect(h.log).toHaveCallOrder([[5], [6], [1], [2], [3], [4]]);
      await Promise.all([
        expect(p1).resolves.toEqual([1]),
        expect(p2).resolves.toEqual([2]),
        expect(p3).resolves.toEqual([3]),
        expect(p4).resolves.toEqual([4]),
        expect(p5).resolves.toEqual([5]),
      ]);
    });
  });
});
