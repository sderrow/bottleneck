import { beforeEach, afterEach, vi } from "vitest";

/** Real setTimeout captured at module load, before any fake-timer install. */
export const realSetTimeout = globalThis.setTimeout;

/** Ground truth from vitest — never track this in module state. */
export function isFakeClock() {
  return vi.isFakeTimers();
}

/**
 * Install vitest fake timers for the `local` project (no-op when DATASTORE is
 * set — never freeze time while real Redis I/O is in flight, and never fake
 * timers in a fork holding a long-lived redis client whose reconnect timers
 * would be discarded by useRealTimers()).
 *
 * Must be called at module top level before any `describe()` so hook-stack ordering
 * matches setup.ts's redis flush (module beforeEach runs after setup beforeEach).
 *
 * Do NOT use `vi.waitFor` or `expect.poll` directly in shared test files — their sync
 * auto-advance corrupts the virtual timeline under fake time. Use `waitForState` instead.
 */
export function useFakeClock() {
  if (process.env.DATASTORE != null) return;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setTimerTickMode("nextTimerAsync");
  });

  afterEach(() => {
    vi.useRealTimers();
  });
}

/** Per-test opt-out from fake timers; next test's beforeEach reinstalls. */
export function useRealClockForThisTest() {
  vi.useRealTimers();
}

/** Promise delay via global setTimeout (respects fake timers when installed). */
export function wait(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
