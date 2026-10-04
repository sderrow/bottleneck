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

/**
 * Strip the "Error" headline from a captured stack, leaving only the frames.
 */
export function cleanScheduleStack(raw: string | undefined): string | undefined {
  if (typeof raw !== "string") {
    return undefined;
  }
  const lines = raw.split("\n");
  if (lines.length > 0 && !/^\s*at\s/.test(lines[0] ?? "")) {
    lines.shift();
  }
  while (lines.length > 0 && (lines[0] ?? "").trim() === "") {
    lines.shift();
  }
  if (lines.length === 0) {
    return undefined;
  }
  return lines.join("\n");
}

/**
 * Where a job was scheduled. `holder.stack` is read only when a task fails,
 * so `Error.prepareStackTrace` (source maps) stays off the schedule() path.
 */
export type ScheduleStackCapture = {
  holder?: { stack?: string };
  /** Cut at the base schedule(), for when an override cutoff captured nothing. */
  fallback?: { stack?: string };
  label: string;
  /** A chained limiter's job: no caller frames, rendered as a "via" label. */
  chained?: boolean;
};

type ScheduleCutoff = (...args: never[]) => unknown;

// Set around the synchronous call that builds a job. Library frames are cut
// by function identity, not file path, so bundled app code is never mistaken
// for library frames.
let scheduleCutoff: ScheduleCutoff | undefined;
let forwardingChain = false;

/** Cut stacks of jobs scheduled in `cb` at `fn`, an entry point like wrap(). */
export function withScheduleCutoff<T>(fn: ScheduleCutoff, cb: () => T): T {
  const previous = scheduleCutoff;
  scheduleCutoff = fn;
  try {
    return cb();
  } finally {
    scheduleCutoff = previous;
  }
}

/** Run `cb`, marking the job it schedules as a chained forward. */
export function forwardChain<T>(cb: () => T): T {
  const previous = forwardingChain;
  forwardingChain = true;
  try {
    return cb();
  } finally {
    forwardingChain = previous;
  }
}

/**
 * Capture where a job is scheduled; undefined when `label` is null (disabled).
 * `base` is the library's schedule(); `override` is the limiter's own
 * `schedule`, which differs when a subclass or instance replaces it.
 */
export function captureScheduleLocation(
  label: string | null,
  base: ScheduleCutoff,
  override: ScheduleCutoff,
): ScheduleStackCapture | undefined {
  const chained = forwardingChain;
  // Consume the flag so a listener scheduling during the forward isn't chained.
  forwardingChain = false;
  if (label == null) {
    return undefined;
  }
  if (chained) {
    return { label, chained: true };
  }
  try {
    const holder: { stack?: string } = {};
    if (typeof Error.captureStackTrace !== "function") {
      holder.stack = new Error().stack;
      return { holder, label };
    }
    // Cutting at an override drops its wrapper frames too. V8 ignores bound
    // functions as cutoffs, so those fall back to `base`.
    const useOverride =
      scheduleCutoff == null && override !== base && !override.name.startsWith("bound ");
    Error.captureStackTrace(holder, scheduleCutoff ?? (useOverride ? override : base));
    if (!useOverride) {
      return { holder, label };
    }
    // An override that isn't on the stack (e.g. `base` called directly)
    // captures nothing.
    const fallback: { stack?: string } = {};
    Error.captureStackTrace(fallback, base);
    return { holder, fallback, label };
  } catch {
    return undefined;
  }
}

const SCHEDULE_MARKER_PREFIX = "From previous Bottleneck.schedule location";
const MAX_SCHEDULE_SECTIONS = 3;

// Per-error state. `seen` skips retries (same capture); `via` holds chained
// labels for the next section; `lastStart`/`written` let the last section be
// replaced in place once capped.
type Annotation = {
  seen: Set<ScheduleStackCapture>;
  via: string[];
  sections: number;
  omitted: number;
  lastStart: number;
  written: string;
};

const annotations = new WeakMap<object, Annotation>();

/**
 * Append where the job was scheduled to a task failure's stack. Nested
 * limiters each add a section, innermost first; past MAX_SCHEDULE_SECTIONS
 * the last section is replaced, so the outermost (the app's call site) is
 * always kept. Returns the original error, mutated in place when possible;
 * non-Error rejections pass through untouched.
 */
export function attachScheduleStack<T>(error: T, capture: ScheduleStackCapture | undefined): T {
  if (capture == null) {
    return error;
  }
  if (typeof error !== "object" || error === null) {
    return error;
  }
  const target = error as { stack?: unknown };
  if (typeof target.stack !== "string") {
    return error;
  }
  let state = annotations.get(error);
  if (state?.seen.has(capture)) {
    return error;
  }
  if (state == null) {
    state = { seen: new Set(), via: [], sections: 0, omitted: 0, lastStart: 0, written: "" };
    annotations.set(error, state);
  }
  state.seen.add(capture);
  if (capture.chained) {
    // Inner limiters fail first: prepend so the list reads outer → inner.
    state.via.unshift(capture.label);
    return error;
  }
  const replacing = state.sections >= MAX_SCHEDULE_SECTIONS;
  if (replacing && target.stack !== state.written) {
    // Edited elsewhere since our last write; slicing could cut that text.
    return error;
  }
  try {
    const frames =
      cleanScheduleStack(capture.holder?.stack) ?? cleanScheduleStack(capture.fallback?.stack);
    if (frames == null) {
      return error;
    }
    const via = state.via.length > 0 ? `, via ${state.via.join(", ")}` : "";
    let section = `${SCHEDULE_MARKER_PREFIX} (${capture.label}${via}):\n${frames}`;
    let base = target.stack;
    if (replacing) {
      const omitted = state.omitted + 1;
      section = `    ... ${omitted} nested schedule location${omitted === 1 ? "" : "s"} omitted\n${section}`;
      base = base.slice(0, state.lastStart);
    }
    const stack = `${base}\n${section}`;
    target.stack = stack;
    state.via = [];
    state.lastStart = base.length;
    state.written = stack;
    if (replacing) {
      state.omitted++;
    } else {
      state.sections++;
    }
  } catch {
    // Throwing prepareStackTrace or read-only stack: leave it untouched.
  }
  return error;
}
