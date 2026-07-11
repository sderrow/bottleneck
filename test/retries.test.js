import { useFakeClock, isFakeClock } from "./helpers/clock.js";
import { test, describe, expect } from "./helpers/test-api.js";

useFakeClock();

const badJob = () => Promise.reject(new Error("boom"));

const assertBackoffs = (attemptTimes, backoffMs) => {
  for (let i = 1; i < attemptTimes.length; i++) {
    const delta = attemptTimes[i] - attemptTimes[i - 1];
    if (isFakeClock()) {
      expect(delta).toBe(backoffMs);
    } else {
      expect(delta).toBeGreaterThanOrEqual(backoffMs - 5);
    }
  }
};

describe("Retries", () => {
  test("Should retry when requested by the user (sync)", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ trackDoneStatus: true });
    let failedEvents = 0;
    let retryEvents = 0;
    const attemptTimes = [];

    limiter.on("failed", (error, info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      expect(info.retryCount).toStrictEqual(failedEvents);
      failedEvents++;
      return 50;
    });

    limiter.on("retry", (_error, _info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      retryEvents++;
    });

    let times = 0;
    const job = () => {
      attemptTimes.push(Date.now());
      times++;
      if (times <= 2) {
        return Promise.reject(new Error("boom"));
      }
      return Promise.resolve("Success!");
    };

    expect(await limiter.schedule(job)).toStrictEqual("Success!");
    expect(failedEvents).toStrictEqual(2);
    expect(retryEvents).toStrictEqual(2);
    expect(attemptTimes.length).toStrictEqual(3);
    assertBackoffs(attemptTimes, 50);
    expect(limiter.counts().EXECUTING).toStrictEqual(0);
    expect(limiter.counts().DONE).toStrictEqual(1);
  });

  test("Should retry when requested by the user (async)", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ trackDoneStatus: true });
    let failedEvents = 0;
    let retryEvents = 0;
    const attemptTimes = [];

    limiter.on("failed", (error, info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      expect(info.retryCount).toStrictEqual(failedEvents);
      failedEvents++;
      return Promise.resolve(50);
    });

    limiter.on("retry", (_error, _info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      retryEvents++;
    });

    let times = 0;
    const job = () => {
      attemptTimes.push(Date.now());
      times++;
      if (times <= 2) {
        return Promise.reject(new Error("boom"));
      }
      return Promise.resolve("Success!");
    };

    expect(await limiter.schedule(job)).toStrictEqual("Success!");
    expect(failedEvents).toStrictEqual(2);
    expect(retryEvents).toStrictEqual(2);
    expect(attemptTimes.length).toStrictEqual(3);
    assertBackoffs(attemptTimes, 50);
    expect(limiter.counts().EXECUTING).toStrictEqual(0);
    expect(limiter.counts().DONE).toStrictEqual(1);
  });

  test("Should not retry when user returns an error (sync)", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ trackDoneStatus: true }, { expectErrors: true });
    let failedEvents = 0;
    let retryEvents = 0;
    let errorEvents = 0;
    let caught = false;

    limiter.on("failed", (error, info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      expect(info.retryCount).toStrictEqual(failedEvents);
      failedEvents++;
      throw new Error("Nope");
    });

    limiter.on("retry", (_error, _info) => {
      retryEvents++;
    });

    limiter.on("error", (error, _info) => {
      expect(error.message).toStrictEqual("Nope");
      errorEvents++;
    });

    try {
      await limiter.schedule(badJob);
      throw new Error("Should not reach");
    } catch (error) {
      expect(error.message).toStrictEqual("boom");
      caught = true;
    }
    expect(failedEvents).toStrictEqual(1);
    expect(retryEvents).toStrictEqual(0);
    expect(errorEvents).toStrictEqual(1);
    expect(caught).toStrictEqual(true);
    expect(limiter.counts().EXECUTING).toStrictEqual(0);
    expect(limiter.counts().DONE).toStrictEqual(1);
  });

  test("Should not retry when user returns an error (async)", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ trackDoneStatus: true }, { expectErrors: true });
    let failedEvents = 0;
    let retryEvents = 0;
    let errorEvents = 0;
    let caught = false;

    limiter.on("failed", (error, info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      expect(info.retryCount).toStrictEqual(failedEvents);
      failedEvents++;
      return Promise.reject(new Error("Nope"));
    });

    limiter.on("retry", (_error, _info) => {
      retryEvents++;
    });

    limiter.on("error", (error, _info) => {
      expect(error.message).toStrictEqual("Nope");
      errorEvents++;
    });

    try {
      await limiter.schedule(badJob);
      throw new Error("Should not reach");
    } catch (error) {
      expect(error.message).toStrictEqual("boom");
      caught = true;
    }
    expect(failedEvents).toStrictEqual(1);
    expect(retryEvents).toStrictEqual(0);
    expect(errorEvents).toStrictEqual(1);
    expect(caught).toStrictEqual(true);
    expect(limiter.counts().EXECUTING).toStrictEqual(0);
    expect(limiter.counts().DONE).toStrictEqual(1);
  });

  test("Should not retry when user returns null (sync)", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ trackDoneStatus: true });
    let failedEvents = 0;
    let retryEvents = 0;
    let caught = false;

    limiter.on("failed", (error, info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      expect(info.retryCount).toStrictEqual(failedEvents);
      failedEvents++;
      return null;
    });

    limiter.on("retry", (_error, _info) => {
      retryEvents++;
    });

    try {
      await limiter.schedule(badJob);
      throw new Error("Should not reach");
    } catch (error) {
      expect(error.message).toStrictEqual("boom");
      caught = true;
    }
    expect(failedEvents).toStrictEqual(1);
    expect(retryEvents).toStrictEqual(0);
    expect(caught).toStrictEqual(true);
    expect(limiter.counts().EXECUTING).toStrictEqual(0);
    expect(limiter.counts().DONE).toStrictEqual(1);
  });

  test("Should not retry when user returns null (async)", async ({ makeLimiter }) => {
    const limiter = makeLimiter({ trackDoneStatus: true });
    let failedEvents = 0;
    let retryEvents = 0;
    let caught = false;

    limiter.on("failed", (error, info) => {
      expect(limiter.counts().EXECUTING).toStrictEqual(1);
      expect(info.retryCount).toStrictEqual(failedEvents);
      failedEvents++;
      return Promise.resolve(null);
    });

    limiter.on("retry", (_error, _info) => {
      retryEvents++;
    });

    try {
      await limiter.schedule(badJob);
      throw new Error("Should not reach");
    } catch (error) {
      expect(error.message).toStrictEqual("boom");
      caught = true;
    }
    expect(failedEvents).toStrictEqual(1);
    expect(retryEvents).toStrictEqual(0);
    expect(caught).toStrictEqual(true);
    expect(limiter.counts().EXECUTING).toStrictEqual(0);
    expect(limiter.counts().DONE).toStrictEqual(1);
  });
});
