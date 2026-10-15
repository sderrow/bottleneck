import BottleneckError from "./BottleneckError";

const fallbackReasons = new WeakMap<AbortSignal, BottleneckError>();

/**
 * What an aborted job rejects with. `reason` is missing before Node 16.14;
 * the fallback is one error per signal, so it compares like a real reason.
 */
export function abortReason(signal: AbortSignal): unknown {
  if (signal.reason !== undefined) return signal.reason;
  let fallback = fallbackReasons.get(signal);
  if (fallback == null) {
    fallback = new BottleneckError("This job was aborted", "ABORTED");
    fallbackReasons.set(signal, fallback);
  }
  return fallback;
}

type Abortable = { _abort(): void };
type Watchers = { jobs: Set<Abortable>; listener: () => void };

// One "abort" listener per signal, however many jobs share it: per-job
// listeners trip MaxListeners warnings on some runtimes, and removing one is
// O(listeners).
const watched = new WeakMap<AbortSignal, Watchers>();

/** Call `job._abort()` when `signal` aborts, until unwatched. */
export function watchAbort(signal: AbortSignal, job: Abortable): void {
  let watchers = watched.get(signal);
  if (watchers == null) {
    const jobs = new Set<Abortable>();
    const listener = (): void => {
      // Unregister first, so jobs unwatching themselves leave `jobs` intact.
      watched.delete(signal);
      for (const j of jobs) j._abort();
    };
    watchers = { jobs, listener };
    watched.set(signal, watchers);
    signal.addEventListener("abort", listener, { once: true });
  }
  watchers.jobs.add(job);
}

export function unwatchAbort(signal: AbortSignal, job: Abortable): void {
  const watchers = watched.get(signal);
  if (watchers == null || !watchers.jobs.delete(job)) return;
  if (watchers.jobs.size === 0) {
    watched.delete(signal);
    signal.removeEventListener("abort", watchers.listener);
  }
}
