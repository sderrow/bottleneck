import { test as baseTest, expect as vitestExpect, vi } from "vitest";
import type BottleneckBase from "../../src/Bottleneck";
import type { IORedisClient, NodeRedisClient } from "../../src/cluster/redis-types";
import type {
  BottleneckOptions,
  IORedisConnectionOptions,
  JobOptions,
  RedisConnectionOptions,
} from "../../src/types";
import Bottleneck from "../bottleneck";
import { isFakeClock } from "./clock";
import { createTaskFns } from "./job-tasks";
import makeLimiterHelper, { closeTestClient, makeTestClient } from "./limiter";
import { wrongType } from "./wrong-type";

export { waitForState } from "./wait-for-state";
export { deferred } from "./job-tasks";

type Limiter = BottleneckBase;
type Group = InstanceType<(typeof Bottleneck)["Group"]>;
type Connection =
  | InstanceType<(typeof BottleneckBase)["RedisConnection"]>
  | InstanceType<(typeof BottleneckBase)["IORedisConnection"]>;
type LimiterOptions = Record<string, unknown>;
type Disconnectable = { disconnect(flush?: boolean): unknown };
type Track = <T extends Disconnectable>(resource: T) => T;

/** Return shape of the `harness` fixture (createJobHarness below). */
export type JobHarness = ReturnType<typeof createJobHarness>;

type MakeLimiter = (opts?: LimiterOptions, meta?: { expectErrors?: boolean }) => Limiter;
type MakeGroup = (opts?: LimiterOptions) => Group;
type Client = NodeRedisClient | IORedisClient;
type MakeClient = (
  clientOptions?: Record<string, unknown>,
  meta?: { expectErrors?: boolean },
) => Client;
type MakeConnection = (opts?: { client: Client; subscriber?: Client }) => Connection;

export interface TestFixtures {
  harness: JobHarness;
  track: Track;
  makeLimiter: MakeLimiter;
  makeGroup: MakeGroup;
  makeClient: MakeClient;
  makeConnection: MakeConnection;
  limiter: Limiter;
  limiterOptions: LimiterOptions;
  limiterMeta: { expectErrors?: boolean };
}

/**
 * Enqueue barrier: resolves once every schedule() issued so far on this
 * limiter has committed to the queue (or been dropped). schedule() returns
 * only the COMPLETION promise, so tests that sequence submissions wait here.
 *
 * This is the one place the suite couples to the private _submitLock; if the
 * library ever hardens privacy (#fields), replace this body with a per-job
 * "queued"/"dropped" event race.
 */
export const enqueued = (limiter: Limiter) => limiter._submitLock.schedule(() => Promise.resolve());

function createJobHarness() {
  const start = Date.now();
  const callTimes: number[] = [];

  const record = vi.fn<(_err: unknown, _result: unknown[]) => void>((_err, _result) => {
    callTimes.push(Date.now() - start);
  });

  const log = {
    record,
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

  function flushLimiter(limiter: Limiter, scheduleOptions?: BottleneckOptions | JobOptions) {
    const opt = scheduleOptions != null ? scheduleOptions : {};
    return limiter.schedule(opt as JobOptions, () => Promise.resolve(getResults()));
  }

  return {
    log: record,
    promise: tasks.promise,
    slowPromise: tasks.slowPromise,
    deferredPromise: tasks.deferredPromise,
    getResults,
    results: getResults,
    flushLimiter,
    callTimes,
  };
}

function callResultArgs(call: unknown[]) {
  const result = call[1];
  return Array.isArray(result) ? result : [result];
}

// Custom matchers are attached to vitest's expect; declare them on the
// Assertion/AsyncAssertion interfaces so `expect(x).toHaveCallOrder(...)`
// typechecks across the suite.
declare module "vitest" {
  interface Assertion {
    toHaveCallOrder(expected: unknown[]): void;
    toHaveFinalCallAt(expectedMs: number, minBound?: number): void;
    toHaveCallAt(index: number, expectedMs: number, minBound?: number): void;
  }
  interface AsymmetricMatchersContaining {
    toHaveCallOrder(expected: unknown[]): void;
  }
}

vitestExpect.extend({
  toHaveCallOrder(this: unknown, received: { mock: { calls: unknown[][] } }, expected: unknown[]) {
    const calls = received.mock.calls;
    const pass =
      calls.length === expected.length &&
      calls.every((call, i) => vitestExpect(callResultArgs(call)).toEqual(expected[i]));

    const message = () =>
      pass
        ? "expected call order not to match"
        : `expected call order ${JSON.stringify(expected).slice(0, 200)}, got ${JSON.stringify(
            calls.map(callResultArgs),
          ).slice(0, 200)}`;

    return { pass, message };
  },

  toHaveFinalCallAt(
    this: unknown,
    received: JobHarness | { callTimes?: number[] },
    expectedMs: number,
    minBound?: number,
  ) {
    const lo = minBound !== undefined ? minBound : 10;
    const { callTimes } = received as { callTimes?: number[] };
    const duration =
      callTimes != null ? callTimes.at(-1) : (received as JobHarness).getResults().callsDuration;

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

  toHaveCallAt(
    this: unknown,
    received: { calls?: { time: number }[]; callTimes?: number[] },
    index: number,
    expectedMs: number,
    minBound?: number,
  ) {
    const lo = minBound !== undefined ? minBound : 5;
    const time = received.calls != null ? received.calls[index]?.time : received.callTimes?.[index];

    if (isFakeClock()) {
      // Accepts N or exactly N+1: sinon fake-timers assigns callAt = now + 1
      // to a 0ms timer created INSIDE a running timer callback ("duringTick"
      // quantization), which every heartbeat-driven dispatch hits via
      // LocalDatastore's heartbeat setInterval -> yieldLoop() sleep(0).
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

export const test = baseTest.extend<{
  harness: JobHarness;
  track: <T extends Disconnectable>(resource: T) => T;
  makeLimiter: MakeLimiter;
  makeGroup: MakeGroup;
  makeClient: MakeClient;
  makeConnection: MakeConnection;
  limiter: Limiter;
  limiterOptions: Record<string, unknown>;
  limiterMeta: { expectErrors?: boolean };
}>({
  // oxlint-disable-next-line no-empty-pattern
  async harness({}, use) {
    await use(createJobHarness());
  },
  limiterOptions: {},
  limiterMeta: {},
  // oxlint-disable-next-line no-empty-pattern
  async track({}, use) {
    const resources: Disconnectable[] = [];
    await use(<T extends Disconnectable>(resource: T): T => {
      resources.push(resource);
      return resource;
    });
    for (const resource of resources.toReversed()) {
      try {
        await resource.disconnect(false);
      } catch {
        // tolerate mid-test disconnects
      }
    }
  },
  async makeLimiter({ track }, use) {
    await use((opts?: LimiterOptions, meta?: { expectErrors?: boolean }) =>
      track(makeLimiterHelper(opts, meta)),
    );
  },
  async makeGroup({ track }, use) {
    await use((opts?: LimiterOptions) =>
      track(new Bottleneck.Group(opts ?? {}) as unknown as Group),
    );
  },
  async makeClient({ track }, use) {
    await use((clientOptions, meta) => {
      const client = makeTestClient(clientOptions, meta);
      track({ disconnect: () => closeTestClient(client) });
      return client;
    });
  },
  async makeConnection({ track, makeClient }, use) {
    await use((opts) => {
      // The fixture picks the connection class for the current datastore, so
      // the client's type isn't tied to it here.
      const options = opts ?? { client: makeClient() };
      const connection =
        process.env.DATASTORE === "ioredis"
          ? new Bottleneck.IORedisConnection(wrongType<IORedisConnectionOptions>(options))
          : new Bottleneck.RedisConnection(wrongType<RedisConnectionOptions>(options));
      return track(connection);
    });
  },
  async limiter({ makeLimiter, limiterOptions, limiterMeta }, use) {
    await use(makeLimiter(limiterOptions, limiterMeta));
  },
});
