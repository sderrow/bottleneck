import type BottleneckBase from "../../src/Bottleneck";
import type { BottleneckOptions } from "../../src/types";
import Bottleneck from "../bottleneck";
import buildClientOptions from "../redis-client-options";
import { logUnexpectedError } from "./clients";

/**
 * Create a Bottleneck limiter pre-wired for the current test environment.
 *
 * @param {object} [options] - Bottleneck constructor options. Without a `connection`, the
 *   `datastore` harness flag defaults to the env's DATASTORE: test/bottleneck.ts builds a
 *   Redis-backed limiter (from `clientOptions`, if given) or a local one from it.
 * @param {{ expectErrors?: boolean }} [meta] - Test-level flags kept separate from Bottleneck options
 * @returns {import("../../src/Bottleneck").default}
 */
function makeLimiter(
  options: Record<string, unknown> = {},
  meta: { expectErrors?: boolean } = {},
): BottleneckBase {
  const assigned = { ...options };
  if (assigned.datastore == null && assigned.connection == null) {
    assigned.datastore = process.env.DATASTORE ?? "local";
  }

  // Harness options (the `datastore` flag, `clientOptions`) are translated
  // into product options by test/bottleneck.ts.
  const limiter = new Bottleneck(assigned as BottleneckOptions);

  if (!meta.expectErrors) {
    limiter.on("error", (err) => logUnexpectedError("makeLimiter", err));
  }

  // makeLimiter is synchronous; suppress the unhandled-rejection from ready()
  // for tests that never await it (connection-failure tests assert via the
  // "error" event instead).
  limiter.ready().catch(() => {});

  return limiter;
}

export default makeLimiter;
export { makeLimiter, buildClientOptions };
