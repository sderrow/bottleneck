import { test as baseTest, expect as vitestExpect, vi } from "vitest";
import { isFakeClock } from "./clock.js";
import { createTaskFns } from "./job-tasks.js";
import makeLimiterHelper from "./limiter.js";

export { waitForState } from "./wait-for-state.js";
export { deferred } from "./job-tasks.js";

function pNoErrVal(promise, ...expected) {
  return promise.then((actual) => {
    vitestExpect(actual).toEqual(expected);
  });
}

function noErrVal(...expected) {
  return (err, ...actual) => {
    vitestExpect(err).toBeNull();
    vitestExpect(actual).toEqual(expected);
  };
}

function createJobHarness() {
  const start = Date.now();
  const callTimes = [];

  const record = vi.fn((_err, _result) => {
    callTimes.push(Date.now() - start);
  });

  const log = {
    record: record,
    calls: record.mock.calls,
  };

  const tasks = createTaskFns(log);

  function getResults() {
    return {
      elapsed: Date.now() - start,
      callsDuration: callTimes.length > 0 ? callTimes.at(-1) : null,
      calls: record.mock.calls.map((call, i) => {
        return { err: call[0], result: call[1], time: callTimes[i] };
      }),
    };
  }

  function flushLimiter(limiter, scheduleOptions) {
    const opt = scheduleOptions != null ? scheduleOptions : {};
    return limiter.schedule(opt, () => {
      return Promise.resolve(getResults());
    });
  }

  return {
    log: record,
    job: tasks.job,
    slowJob: tasks.slowJob,
    deferredJob: tasks.deferredJob,
    promise: tasks.promise,
    slowPromise: tasks.slowPromise,
    deferredPromise: tasks.deferredPromise,
    getResults: getResults,
    results: getResults,
    flushLimiter: flushLimiter,
    pNoErrVal: pNoErrVal,
    noErrVal: noErrVal,
    callTimes: callTimes,
  };
}

function callResultArgs(call) {
  const result = call[1];
  return Array.isArray(result) ? result : [result];
}

vitestExpect.extend({
  toHaveCallOrder(received, expected) {
    const calls = received.mock.calls;
    const pass =
      calls.length === expected.length &&
      expected.every((order, i) => this.equals(callResultArgs(calls[i]), order));

    const message = () =>
      pass
        ? "expected call order not to match"
        : `expected call order ${this.utils.printExpected(expected)}, got ${this.utils.printReceived(calls.map(callResultArgs))}`;

    return { pass, message };
  },

  toHaveFinalCallAt(received, expectedMs, minBound) {
    const lo = minBound !== undefined ? minBound : 10;
    const duration =
      received.callTimes != null ? received.callTimes.at(-1) : received.getResults().callsDuration;

    if (isFakeClock()) {
      const pass = duration === expectedMs;
      const message = () =>
        pass
          ? `expected final call not to be at exactly ${expectedMs}ms`
          : `expected final call at exactly ${expectedMs}ms, got ${duration}ms`;
      return { pass, message };
    }

    const min = expectedMs - lo;
    const pass = duration != null && duration > min;
    const message = () =>
      pass
        ? `expected final call not to be after ${min}ms`
        : `expected final call after ${min}ms, got ${duration}ms`;

    return { pass, message };
  },

  toHaveCallAt(received, index, expectedMs, minBound) {
    const lo = minBound !== undefined ? minBound : 5;
    const time = received.calls != null ? received.calls[index]?.time : received.callTimes?.[index];

    if (isFakeClock()) {
      // Accepts N or exactly N+1: sinon fake-timers assigns callAt = now + 1
      // to a 0ms timer created INSIDE a running timer callback ("duringTick"
      // quantization), which every heartbeat-driven dispatch hits via
      // LocalDatastore's heartbeat setInterval -> yieldLoop() setTimeout(0).
      // Interval-driven calls therefore land at exactly N+1; direct minTime
      // dispatches land at exactly N. Never N-1, never N+2.
      const pass = time === expectedMs || time === expectedMs + 1;
      const message = () =>
        pass
          ? `expected call ${index} not to be at ${expectedMs}ms or ${expectedMs + 1}ms`
          : `expected call ${index} at ${expectedMs}ms (or +1ms fake-timer quantization), got ${time}ms`;

      return { pass, message };
    }

    const min = expectedMs - lo;
    const pass = time != null && time >= min;
    const message = () =>
      pass
        ? `expected call ${index} not to be after ${min}ms`
        : `expected call ${index} after ${min}ms, got ${time}ms`;

    return { pass, message };
  },
});

export const test = baseTest.extend({
  // Vitest requires fixture functions to destructure their first argument —
  // it parses the pattern to build the dependency graph — so the empty
  // pattern is mandatory for dependency-free fixtures.
  // oxlint-disable-next-line no-empty-pattern
  harness: async function ({}, use) {
    await use(createJobHarness());
  },
  limiterOptions: {},
  limiterMeta: {},
  // oxlint-disable-next-line no-empty-pattern
  track: async function ({}, use) {
    const resources = [];
    await use((resource) => {
      resources.push(resource);
      return resource;
    });
    for (let i = resources.length - 1; i >= 0; i--) {
      try {
        await resources[i].disconnect(false);
      } catch {
        // tolerate mid-test disconnects
      }
    }
  },
  makeLimiter: async function ({ track }, use) {
    await use((opts, meta) => track(makeLimiterHelper(opts, meta)));
  },
  limiter: async function ({ makeLimiter, limiterOptions, limiterMeta }, use) {
    await use(makeLimiter(limiterOptions, limiterMeta));
  },
});

export const expect = vitestExpect;
export { describe, vi } from "vitest";
