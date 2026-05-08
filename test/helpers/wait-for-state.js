import { vi } from "vitest";

/**
 * Thin wrapper around `vi.waitFor` with project-tuned defaults.
 *
 * Bottleneck tests poll for transient state windows (state machine transitions
 * gated by `minTime`, redis pub/sub round-trips, etc.) that can be much narrower
 * than `vi.waitFor`'s 50ms default polling interval. Defaulting `interval: 10`
 * keeps polling tight enough to observe those windows reliably; `timeout: 2000`
 * matches the previous handcrafted helper.
 *
 * Same shape as `vi.waitFor`: pass an `options` object to override either
 * default for a specific call site.
 */
const DEFAULTS = { timeout: 2000, interval: 10 };

export function waitForState(callback, options) {
  return vi.waitFor(callback, { ...DEFAULTS, ...options });
}
