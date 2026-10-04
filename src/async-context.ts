import { AsyncResource } from "node:async_hooks";

/**
 * Bind a scheduled task to the async context (e.g. AsyncLocalStorage state)
 * active when schedule() is called, so the task observes that context when it
 * eventually runs — no matter which tick, timer, or queued drain executes it.
 *
 * Falls back to the unbound task outside Node (the light/browser build stubs
 * this module out entirely).
 */
export function bindTask<T extends (...args: any[]) => unknown>(task: T): T {
  try {
    return AsyncResource.bind(task);
  } catch {
    return task;
  }
}

/**
 * Capture the async context (e.g. AsyncLocalStorage state) active right now
 * as a resource that can be re-entered later from a foreign async context.
 * Used for the chained limiter's schedule() path in `Job.doExecute`, which
 * runs from this job's timer context instead of the schedule-time context.
 *
 * Returns undefined outside Node (the light/browser build shims
 * `node:async_hooks` without an AsyncResource constructor).
 */
export function captureAsyncResource(): AsyncResource | undefined {
  try {
    return new AsyncResource("Bottleneck.Job");
  } catch {
    return undefined;
  }
}

/**
 * Run `fn` inside a resource captured with `captureAsyncResource`.
 * Falls back to a direct call when there is no resource.
 */
export function runWithAsyncResource<T>(resource: AsyncResource | undefined, fn: () => T): T {
  if (resource != null) {
    return resource.runInAsyncScope(fn);
  }
  return fn();
}

// Captured at module load, which in practice precedes any caller context.
//
// Load-order caveat: the "detached" context is whatever is active when this
// module is first loaded. Import bottleneck at process startup, before
// entering any request/trace context. A first load from inside a request
// (e.g. a lazy `import()` in a request handler) would pin that request's
// context as "detached" for every limiter in the process. Node offers no
// public API for a truly empty context, so this cannot be fixed in-library.
const detachedResource = captureAsyncResource();

/**
 * Run `fn` outside the caller's async context (see load-order caveat above).
 * Used for work whose lifetime outlives its constructing caller: Redis
 * connection/socket setup, whose pub/sub callbacks would otherwise keep
 * draining jobs into the constructing request's trace.
 */
export function runDetached<T>(fn: () => T): T {
  return runWithAsyncResource(detachedResource, fn);
}

/**
 * `setInterval`, started outside the caller's async context. Timers owned by a
 * limiter or group (heartbeats, group cleanup) outlive whichever caller
 * happened to construct them (e.g. a Group key first created inside a traced
 * request), so they must neither observe nor retain that caller's context.
 */
export function setDetachedInterval(
  callback: () => unknown,
  ms: number,
): ReturnType<typeof setInterval> {
  return runWithAsyncResource(detachedResource, () => setInterval(callback, ms));
}

/**
 * `setTimeout`, started outside the caller's async context. Same rationale
 * as `setDetachedInterval`; covers one-shot background timers such as the
 * capacity-priority blacklist delay.
 */
export function setDetachedTimeout(
  callback: () => unknown,
  ms: number,
): ReturnType<typeof setTimeout> {
  return runWithAsyncResource(detachedResource, () => setTimeout(callback, ms));
}
