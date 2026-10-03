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
 * Lazily-captured schedule location. The holders are created with
 * `Error.captureStackTrace` at schedule() time without reading `.stack`,
 * so `Error.prepareStackTrace` (source-map-support under @swc-node/register)
 * only runs when an error actually needs the schedule location. Queued jobs
 * hold onto the unformatted frames until then.
 */
export type ScheduleStackCapture = {
  /** Primary capture, cut at the outermost `schedule` the caller used. */
  holder: { stack?: string };
  /** Fallback capture, cut at `Job`, used when the primary comes back empty. */
  fallback?: { stack?: string };
  /** Marker label: the limiter id, or the owning Group id for Group children. */
  label: string;
};

const SCHEDULE_MARKER_PREFIX = "From previous Bottleneck.schedule location";
const MAX_SCHEDULE_SECTIONS = 3;

function scheduleMarker(label: string): string {
  return `${SCHEDULE_MARKER_PREFIX} (${label}):`;
}

function countScheduleSections(stack: string): number {
  let count = 0;
  let index = 0;
  while ((index = stack.indexOf(SCHEDULE_MARKER_PREFIX, index)) !== -1) {
    count++;
    index += SCHEDULE_MARKER_PREFIX.length;
  }
  return count;
}

// Directory containing this library's own modules, detected from a load-time
// stack frame. Schedule-stack trimming matches the fork's own frames by this
// path instead of by function name, so app code that happens to mention
// `Job` (a model class, `/models/Job.ts`, ...) is never mistaken for limiter
// machinery. In the bundled builds every library frame shares the dist file;
// in source each library file shares this directory.
const ownDir: string | undefined = (() => {
  try {
    const stack = new Error().stack ?? "";
    for (const line of stack.split("\n")) {
      const match = /\(\s*(.*?):\d+:\d+\s*\)/.exec(line) ?? /at\s+(.*?):\d+:\d+/.exec(line);
      const path = match?.[1]?.trim();
      if (path != null && path !== "" && !path.startsWith("node:")) {
        const slash = Math.max(path.lastIndexOf("/"), path.lastIndexOf("\\"));
        if (slash > 0) {
          return path.slice(0, slash);
        }
        return undefined;
      }
    }
  } catch {
    // Ignore: trimming is best-effort without it.
  }
  return undefined;
})();

function isOwnFrame(line: string): boolean {
  if (ownDir == null) {
    return false;
  }
  return line.includes(`${ownDir}/`) || line.includes(`${ownDir}\\`);
}

/**
 * Trim the fork's own machinery from a captured schedule stack. The inner job
 * of a chained pair is scheduled from the outer job's `doExecute`, so its
 * capture trails off into library frames; the next appended section stands in
 * for those, so cut them. Keeps the first frame (the `chained.schedule()`
 * call site) and drops everything from the first subsequent own frame on.
 * Node internals (`processTicksAndRejections`, timers, ...) are left alone:
 * in async stacks the useful frames come after them.
 */
function trimScheduleStack(cleaned: string): string {
  const lines = cleaned.split("\n");
  for (let i = 1; i < lines.length; i++) {
    if (isOwnFrame(lines[i] ?? "")) {
      return trimTrailingOwnFrames(lines.slice(0, i)).join("\n");
    }
  }
  return trimTrailingOwnFrames(lines).join("\n");
}

function trimTrailingOwnFrames(lines: string[]): string[] {
  // Never drop the first frame: for a chained inner job the call site itself
  // lives inside the library, and it is the section's whole point.
  while (lines.length > 1 && isOwnFrame(lines[lines.length - 1] ?? "")) {
    lines.pop();
  }
  return lines;
}

// Error instances already annotated with these capture holders (by holder
// identity). A retry reuses the same job — and the same holder — so the
// second `_onFailure` for the same error is a no-op, while a nested limiter
// carries a different holder and appends its own section.
const appendedCaptures = new WeakMap<object, Set<object>>();

/**
 * Append the schedule-time stack to a task failure, so the rejection shows
 * both where the task threw and where schedule() was called. The capture is
 * formatted here — lazily, on the error path — so the schedule fast path
 * never pays for `Error.prepareStackTrace` / source-map-support. Nested
 * limiters each append their own section (up to MAX_SCHEDULE_SECTIONS);
 * retries reusing the same job/holder annotate only once. Returns the
 * original error (mutated in place when possible). Non-Error rejections and
 * errors without a formatted stack pass through untouched, preserving
 * rejection identity.
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
  const seen = appendedCaptures.get(error);
  if (seen?.has(capture.holder)) {
    return error;
  }
  if (countScheduleSections(target.stack) >= MAX_SCHEDULE_SECTIONS) {
    return error;
  }
  let raw: string | undefined;
  try {
    raw = capture.holder.stack;
  } catch {
    return error;
  }
  let cleaned = cleanScheduleStack(raw);
  if (cleaned == null && capture.fallback != null) {
    try {
      raw = capture.fallback.stack;
    } catch {
      return error;
    }
    cleaned = cleanScheduleStack(raw);
  }
  if (cleaned == null) {
    return error;
  }
  const section = trimScheduleStack(cleaned);
  if (section.trim() === "") {
    return error;
  }
  try {
    target.stack = `${target.stack}\n${scheduleMarker(capture.label)}\n${section}`;
  } catch {
    // Frozen error or read-only stack: leave the original untouched.
  }
  let set = seen;
  if (set == null) {
    set = new Set();
    appendedCaptures.set(error, set);
  }
  set.add(capture.holder);
  return error;
}
