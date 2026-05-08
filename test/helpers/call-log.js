import { expect } from "vitest";

function wait(ms) {
  return new Promise(function (resolve) {
    setTimeout(resolve, ms);
  });
}

/**
 * Call history + timing + assertions for integration tests.
 * Pair with {@link createTaskFns} from "./job-tasks.js" or use {@link createJobHarness}.
 */
export function createCallLog() {
  const start = Date.now();
  const calls = [];

  function record(err, result) {
    calls.push({ err: err, result: result, time: Date.now() - start });
  }

  function getResults() {
    return {
      elapsed: Date.now() - start,
      callsDuration: calls.length > 0 ? calls.at(-1).time : null,
      calls: calls,
    };
  }

  function checkResultsOrder(order) {
    expect(order.length).toBe(calls.length);
    for (let i = 0; i < calls.length; i++) {
      expect(calls[i].result).toEqual(order[i]);
    }
  }

  function checkDuration(shouldBe, minBound, maxBound) {
    const lo = minBound !== undefined ? minBound : 10;
    const hi = maxBound !== undefined ? maxBound : 1000;
    const results = getResults();
    const min = shouldBe - lo;
    const max = shouldBe + hi;
    expect(results.callsDuration).toBeGreaterThan(min);
    expect(results.callsDuration).toBeLessThan(max);
  }

  function pNoErrVal(promise, ...expected) {
    return promise.then(function (actual) {
      expect(actual).toEqual(expected);
    });
  }

  function noErrVal(...expected) {
    return function (err, ...actual) {
      expect(err).toBeNull();
      expect(actual).toEqual(expected);
    };
  }

  return {
    record: record,
    getResults: getResults,
    results: getResults,
    checkResultsOrder: checkResultsOrder,
    checkDuration: checkDuration,
    pNoErrVal: pNoErrVal,
    noErrVal: noErrVal,
    wait: wait,
    calls: calls,
  };
}

/**
 * Schedule a barrier job and resolve with {@link createCallLog}'s snapshot (same as legacy `c.last()`).
 */
export function flushLimiter(limiter, getResults, scheduleOptions) {
  const opt = scheduleOptions != null ? scheduleOptions : {};
  return limiter.schedule(opt, function () {
    return Promise.resolve(getResults());
  });
}
