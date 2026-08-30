import { vi } from "vitest";
import { realSetTimeout } from "./clock.js";

/**
 * Poll until `callback` succeeds (does not throw).
 *
 * Real mode: delegates to `vi.waitFor`.
 *
 * Fake mode: steps the clock one timer batch at a time between predicate
 * evaluations. Stepping (rather than passively yielding to the nextTimerAsync
 * auto-tick) is load-bearing: our next advance is issued from a microtask,
 * which beats the auto-tick pump's next macroturn — so the predicate observes
 * the state after EVERY timer batch. A passive real-macrotask yield loses that
 * race and lets the pump fire several timers between polls, skipping the
 * transient windows callers poll for.
 *
 * Known limits (inherent to vitest's API):
 * - advanceTimersToNextTimerAsync fires ALL timers due at the same virtual
 *   instant atomically — a window that exists only between same-instant timers
 *   is unobservable. Don't write predicates that need one.
 * - An async predicate that itself awaits fake timers (e.g. limiter.running())
 *   parks on real macrotasks, during which the auto-tick pump may fire timers.
 *   Poll stable held states (deferred-hold pattern), not racing ones.
 *
 * Redis-backed projects get a larger default timeout: cross-instance state
 * propagates over real network round-trips, and the documented Docker-proxy
 * stall can shift an entire dispatch chain by ~5000ms (see
 * test/redis-client-options.js) — the window must absorb one full stall with
 * margin while staying below the redis projects' 15s testTimeout so the poll's
 * assertion diff (not vitest's opaque timeout) reports the failure.
 */
const DEFAULTS = {
  timeout: process.env.DATASTORE != null ? 10_000 : 2000,
  interval: 10,
};

// Under fake time, yield a real macrotask after this many consecutive timer
// advances. A predicate that keeps failing over a dense repeating interval
// (heartbeats every 75ms across an unbounded virtual span) would otherwise
// monopolize the CPU for the entire real-time deadline.
const ADVANCES_PER_YIELD = 50;

export function waitForState(callback, options) {
  const opts = { ...DEFAULTS, ...options };

  if (!vi.isFakeTimers()) {
    return vi.waitFor(callback, opts);
  }

  return pollUnderFakeClock(callback, opts);
}

async function pollUnderFakeClock(callback, opts) {
  // Deadline on the real clock: Date/performance/hrtime are all faked.
  const deadline = vi.getRealSystemTime() + opts.timeout;
  let advances = 0;

  for (;;) {
    try {
      return await callback();
    } catch (err) {
      // Throw the freshest error so the failure shows the final state.
      if (vi.getRealSystemTime() >= deadline) {
        throw err;
      }
    }

    if (vi.getTimerCount() > 0 && ++advances % ADVANCES_PER_YIELD !== 0) {
      await vi.advanceTimersToNextTimerAsync();
    } else {
      await new Promise((resolve) => {
        realSetTimeout(resolve, opts.interval);
      });
    }
  }
}
