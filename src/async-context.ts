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
 * Lazily-captured schedule location. The holder is filled with
 * `Error.captureStackTrace` at schedule() time without reading `.stack`, so
 * `Error.prepareStackTrace` (source-map-support under @swc-node/register)
 * only runs when an error actually needs the schedule location. Queued jobs
 * hold onto the unformatted frames until then.
 */
export type ScheduleStackCapture = {
  /** Frames cut at the public entry point, so the first frame is the caller. */
  holder?: { stack?: string };
  /** Marker label: the limiter id, or a configured `scheduleStackLabel`. */
  label: string;
  /**
   * Set for a chained limiter's job, which is scheduled from the outer job's
   * `doExecute` and so never has caller frames of its own. Rendered as a
   * "via" label on the next section instead of a section of its own.
   */
  chained?: boolean;
};

type ScheduleCutoff = (...args: never[]) => unknown;

// Library frames are cut by function identity (`Error.captureStackTrace`'s
// cutoff), never by file path, so bundling bottleneck together with app code
// can't make app frames look like library frames. Both are set only around a
// synchronous call that constructs a job.
let scheduleCutoff: ScheduleCutoff | undefined;
let forwardingChain = false;

/**
 * Run `cb` with `fn` as the stack cutoff for jobs it schedules. Used by
 * public entry points layered over schedule() (e.g. `wrap()`), so their own
 * frames are cut along with schedule()'s.
 */
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
 * Capture the schedule location for a new job, or undefined when `label` is
 * null (capture disabled). `defaultCutoff` is the schedule() implementation,
 * which is always on the stack when a job is built; frames above it (and it)
 * are omitted.
 */
export function captureScheduleLocation(
  label: string | null,
  defaultCutoff: ScheduleCutoff,
): ScheduleStackCapture | undefined {
  const chained = forwardingChain;
  // Consume the flag: anything else scheduled synchronously during the
  // forward (e.g. from an event listener) is a regular schedule() call.
  forwardingChain = false;
  if (label == null) {
    return undefined;
  }
  if (chained) {
    return { label, chained: true };
  }
  try {
    const holder: { stack?: string } = {};
    if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(holder, scheduleCutoff ?? defaultCutoff);
    } else {
      holder.stack = new Error().stack;
    }
    return { holder, label };
  } catch {
    return undefined;
  }
}

const SCHEDULE_MARKER_PREFIX = "From previous Bottleneck.schedule location";
const MAX_SCHEDULE_SECTIONS = 3;

type Annotation = {
  /** Captures already applied: a retry reuses the same job and capture. */
  seen: Set<ScheduleStackCapture>;
  /** Chained limiter labels waiting for the next section. */
  via: string[];
  sections: number;
  /** Middle sections replaced once the cap is reached. */
  omitted: number;
  /** Where the last section (and any omitted note before it) starts. */
  lastStart: number;
  /** The stack as last written here, to detect outside edits. */
  written: string;
};

const annotations = new WeakMap<object, Annotation>();

/**
 * Append the schedule-time stack to a task failure, so the rejection shows
 * both where the task threw and where schedule() was called. The capture is
 * formatted here — lazily, on the error path — so the schedule fast path
 * never pays for `Error.prepareStackTrace` / source-map-support.
 *
 * Nested limiters each append their own section, innermost first, since the
 * innermost job fails first. Past MAX_SCHEDULE_SECTIONS, each new section
 * replaces the last one, so the outermost section (the app's original call
 * site) is always kept and the middle is summarized as omitted. Chained
 * limiters are listed as "via" labels on the next section instead.
 *
 * Returns the original error (mutated in place when possible). Non-Error
 * rejections and errors without a formatted stack pass through untouched,
 * preserving rejection identity.
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
    // Something else edited the stack since our last section; slicing at
    // lastStart could cut its text, so leave the stack alone.
    return error;
  }
  try {
    const frames = cleanScheduleStack(capture.holder?.stack);
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
    // Throwing prepareStackTrace, frozen error, or read-only stack: leave the
    // original untouched.
  }
  return error;
}
