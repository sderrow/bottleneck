// The frame runTask adds. Only the sync stack carries it: after an await in
// the task, the capture ends in `at async Job.doExecute` instead.
const TASK_RUNNER_FRAME = /^\s*at (?:\S+\.)?__bottleneckRunTask\b/;

// A method, since minifiers keep property names but rename functions.
const taskRunner = {
  __bottleneckRunTask(this: unknown, task: (...args: never[]) => unknown, args: never[]): unknown {
    return task.apply(this, args);
  },
};

/**
 * Call `task`. Its frame marks where library frames begin in the schedule
 * stack of a job scheduled from inside the task. Pass it straight to
 * `runInAsyncScope` so it replaces that call's wrapper frame instead of
 * adding one.
 */
export const runTask = taskRunner.__bottleneckRunTask;

/**
 * Strip the "Error" headline from a captured stack, leaving only the frames,
 * and the library frames below a task that scheduled the job.
 */
function cleanScheduleStack(raw: string | undefined): string | undefined {
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
  // Scheduled from inside a task: the frames from the task runner down are
  // the library running that task, which the next section stands in for.
  const runner = lines.findIndex((line) => TASK_RUNNER_FRAME.test(line));
  if (runner > 0) {
    lines.length = runner;
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
    // Cutting at an override drops its wrapper frames too. V8 doesn't cut at
    // a bound function: the capture would start inside bottleneck and isn't
    // empty, so the fallback can't catch it. Bound overrides use `base`.
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
